use std::fmt::Debug;

use itertools::zip_eq;

pub fn fields_eq<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = FStr>,
    FStr: AsRef<str> + Debug,
{
    let mut count = 0;
    let mut eq = Some(quote! {|a: &#record_type, b: &#record_type|}.to_string())
        .into_iter()
        .chain(fields.into_iter().enumerate().map(|(i, field)| {
            count += 1;
            let and = if i > 0 { " && " } else { "" };
            format!(
                "{and}a.{field}().eq(b.{field}())",
                and = and,
                field = field.as_ref()
            )
        }))
        .collect::<String>();
    if count == 0 {
        eq = quote! {|_a: &#record_type, _b: &#record_type| true}.to_string();
    }
    syn::parse_str::<syn::Expr>(&eq).expect("eq")
}

pub fn fields_eq_ab<F, FStr, G, GStr>(
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
    let mut count = 0;
    let mut eq = Some(quote! {|a: &#record_type_a, b: &#record_type_b|}.to_string())
        .into_iter()
        .chain(
            zip_eq(fields_a, fields_b)
                .enumerate()
                .map(|(i, (field_a, field_b))| {
                    count += 1;
                    let and = if i > 0 { " && " } else { "" };
                    format!(
                        "{and}a.{field_a}().eq(b.{field_b}())",
                        and = and,
                        field_a = field_a.as_ref(),
                        field_b = field_b.as_ref()
                    )
                }),
        )
        .collect::<String>();
    if count == 0 {
        eq = quote! {|_a: &#record_type_a, _b: &#record_type_b| true}.to_string();
    }
    syn::parse_str::<syn::Expr>(&eq).expect("eq")
}

#[cfg(test)]
mod tests {
    use super::{fields_eq, fields_eq_ab};

    #[test]
    fn test_fields_eq_1() {
        let expr = fields_eq(&syn::parse_str::<syn::Type>("Record").unwrap(), ["foo"]);
        assert_eq!(
            "| a : & Record , b : & Record | a . foo () . eq (b . foo ())",
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_eq_2() {
        let expr = fields_eq(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            ["foo", "bar"],
        );
        assert_eq!(
            concat!(
                "| a : & Record , b : & Record | a . foo () . eq (b . foo ())",
                " && a . bar () . eq (b . bar ())"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_eq_3() {
        let expr = fields_eq(
            &syn::parse_str::<syn::Type>("Record").unwrap(),
            ["foo", "bar", "foobar"],
        );
        assert_eq!(
            concat!(
                "| a : & Record , b : & Record | a . foo () . eq (b . foo ())",
                " && a . bar () . eq (b . bar ())",
                " && a . foobar () . eq (b . foobar ())"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_eq_ab_1() {
        let expr = fields_eq_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b"],
        );
        assert_eq!(
            "| a : & A , b : & B | a . foo_a () . eq (b . foo_b ())",
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_eq_ab_2() {
        let expr = fields_eq_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a", "bar_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b", "bar_b"],
        );
        assert_eq!(
            concat!(
                "| a : & A , b : & B | a . foo_a () . eq (b . foo_b ())",
                " && a . bar_a () . eq (b . bar_b ())"
            ),
            quote! {#expr}.to_string()
        );
    }

    #[test]
    fn test_fields_eq_ab_3() {
        let expr = fields_eq_ab(
            &syn::parse_str::<syn::Type>("A").unwrap(),
            ["foo_a", "bar_a", "foobar_a"],
            &syn::parse_str::<syn::Type>("B").unwrap(),
            ["foo_b", "bar_b", "foobar_b"],
        );
        assert_eq!(
            concat!(
                "| a : & A , b : & B | a . foo_a () . eq (b . foo_b ())",
                " && a . bar_a () . eq (b . bar_b ())",
                " && a . foobar_a () . eq (b . foobar_b ())"
            ),
            quote! {#expr}.to_string()
        );
    }
}
