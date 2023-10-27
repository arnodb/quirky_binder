use itertools::zip_eq;
use std::{
    fmt::{Debug, Display, Formatter},
    ops::Deref,
};
use truc::record::definition::DatumId;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Debug)]
pub enum Directed<T> {
    Ascending(T),
    Descending(T),
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
            Self::Descending(_) => true,
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
        F: Fn(T) -> U,
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

pub trait Directable: Sized {
    fn asc(self) -> Directed<Self> {
        Directed::Ascending(self)
    }

    fn desc(self) -> Directed<Self> {
        Directed::Descending(self)
    }
}

impl Directable for &str {}
impl Directable for DatumId {}

pub fn fields_cmp<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = Directed<FStr>>,
    FStr: AsRef<str>,
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
                "{then}a.{field}().cmp(b.{field}(){reverse}{end_then})",
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
    FStr: AsRef<str>,
    G: IntoIterator<Item = GStr>,
    GStr: AsRef<str>,
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
                        "{then}a.{field_a}().cmp(b.{field_b}(){end_then})",
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
