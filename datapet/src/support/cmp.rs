use std::{
    fmt::{Debug, Display, Formatter},
    ops::Deref,
};

use itertools::zip_eq;
use serde::Deserialize;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Deserialize, Debug)]
#[serde(from = "MaybeTaggedDirected<T>")]
pub enum Directed<T> {
    Ascending(T),
    Descending(T),
}

// This requires ron 0.9 (https://github.com/ron-rs/ron/issues/492)
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum MaybeTaggedDirected<T> {
    Tagged(TaggedDirected<T>),
    Untagged(T),
}

#[derive(Deserialize, Debug)]
enum TaggedDirected<T> {
    Ascending(T),
    Descending(T),
}

impl<T> From<MaybeTaggedDirected<T>> for Directed<T> {
    fn from(value: MaybeTaggedDirected<T>) -> Self {
        match value {
            MaybeTaggedDirected::Tagged(TaggedDirected::Ascending(t))
            | MaybeTaggedDirected::Untagged(t) => Directed::Ascending(t),
            MaybeTaggedDirected::Tagged(TaggedDirected::Descending(t)) => Directed::Descending(t),
        }
    }
}

impl<T: Display> Display for Directed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending(t) => f.write_fmt(format_args!("asc({})", t)),
            Self::Descending(t) => f.write_fmt(format_args!("desc({})", t)),
        }
    }
}

impl<T> Directed<T> {
    pub fn is_asc(&self) -> bool {
        match self {
            Self::Ascending(_) => true,
            Self::Descending(_) => false,
        }
    }

    pub fn as_ref(&self) -> Directed<&T> {
        match self {
            Directed::Ascending(t) => Directed::Ascending(t),
            Directed::Descending(t) => Directed::Descending(t),
        }
    }

    pub fn map<U, F>(self, f: F) -> Directed<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Directed::Ascending(t) => Directed::Ascending(f(t)),
            Directed::Descending(t) => Directed::Descending(f(t)),
        }
    }
}

impl<T> Deref for Directed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Ascending(t) => t,
            Self::Descending(t) => t,
        }
    }
}

pub fn fields_cmp<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = Directed<FStr>>,
    FStr: AsRef<str> + Debug,
{
    let cmp = Some(quote! {|a: &#record_type, b: &#record_type|}.to_string())
        .into_iter()
        .chain(fields.into_iter().enumerate().map(|(i, field)| {
            let (then, end_then) = if i > 0 {
                (".then_with(|| ", ")")
            } else {
                ("", "")
            };
            let reverse = if field.is_asc() { "" } else { ".reverse()" };
            format!(
                "{then}a.{field}().cmp(b.{field}()){reverse}{end_then}",
                then = then,
                end_then = end_then,
                field = (*field.as_ref()).as_ref(),
                reverse = reverse
            )
        }))
        .collect::<String>();
    syn::parse_str::<syn::Expr>(&cmp).expect("cmp")
}

pub fn fields_cmp_ab<F, FStr, G, GStr>(
    record_type_a: &syn::Type,
    fields_a: F,
    record_type_b: &syn::Type,
    fields_b: G,
) -> syn::Expr
where
    F: IntoIterator<Item = FStr>,
    FStr: AsRef<str> + Debug,
    G: IntoIterator<Item = GStr>,
    GStr: AsRef<str> + Debug,
{
    let cmp = Some(quote! {|a: &#record_type_a, b: &#record_type_b|}.to_string())
        .into_iter()
        .chain(
            zip_eq(fields_a, fields_b)
                .enumerate()
                .map(|(i, (field_a, field_b))| {
                    let (then, end_then) = if i > 0 {
                        (".then_with(|| ", ")")
                    } else {
                        ("", "")
                    };
                    format!(
                        "{then}a.{field_a}().cmp(b.{field_b}()){end_then}",
                        then = then,
                        end_then = end_then,
                        field_a = field_a.as_ref(),
                        field_b = field_b.as_ref()
                    )
                }),
        )
        .collect::<String>();
    syn::parse_str::<syn::Expr>(&cmp).expect("cmp")
}

#[cfg(test)]
mod tests {
    use super::{fields_cmp, fields_cmp_ab, Directed};

    #[test]
    fn test_fields_cmp_1() {
        let expr = fields_cmp(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            [Directed::Ascending("foo")],
        );
        assert_eq!(
            "| a : & Record , b : & Record | a . foo () . cmp (b . foo ())",
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_2() {
        let expr = fields_cmp(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            [Directed::Ascending("foo"), Directed::Ascending("bar")],
        );
        assert_eq!(
            concat!(
                "| a : & Record , b : & Record | a . foo () . cmp (b . foo ())",
                " . then_with (| | a . bar () . cmp (b . bar ()))"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_3() {
        let expr = fields_cmp(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            [
                Directed::Ascending("foo"),
                Directed::Descending("bar"),
                Directed::Descending("foobar"),
            ],
        );
        assert_eq!(
            concat!(
                "| a : & Record , b : & Record | a . foo () . cmp (b . foo ())",
                " . then_with (| | a . bar () . cmp (b . bar ()) . reverse ())",
                " . then_with (| | a . foobar () . cmp (b . foobar ()) . reverse ())"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_4() {
        let expr = fields_cmp(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            [
                Directed::Descending("foo"),
                Directed::Descending("bar"),
                Directed::Descending("foobar"),
            ],
        );
        assert_eq!(
            concat!(
                "| a : & Record , b : & Record | a . foo () . cmp (b . foo ()) . reverse ()",
                " . then_with (| | a . bar () . cmp (b . bar ()) . reverse ())",
                " . then_with (| | a . foobar () . cmp (b . foobar ()) . reverse ())"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_ab_1() {
        let expr = fields_cmp_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b"],
        );
        assert_eq!(
            "| a : & A , b : & B | a . foo_a () . cmp (b . foo_b ())",
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_ab_2() {
        let expr = fields_cmp_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a", "bar_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b", "bar_b"],
        );
        assert_eq!(
            concat!(
                "| a : & A , b : & B | a . foo_a () . cmp (b . foo_b ())",
                " . then_with (| | a . bar_a () . cmp (b . bar_b ()))"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_cmp_ab_3() {
        let expr = fields_cmp_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a", "bar_a", "foobar_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b", "bar_b", "foobar_b"],
        );
        assert_eq!(
            concat!(
                "| a : & A , b : & B | a . foo_a () . cmp (b . foo_b ())",
                " . then_with (| | a . bar_a () . cmp (b . bar_b ()))",
                " . then_with (| | a . foobar_a () . cmp (b . foobar_b ()))"
            ),
            quote! {#expr}.to_string()
        );
    }
}
