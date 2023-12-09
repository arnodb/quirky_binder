use itertools::zip_eq;

pub fn fields_eq<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = FStr>,
    FStr: AsRef<str>,
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
    FStr: AsRef<str>,
    G: IntoIterator<Item = GStr>,
    GStr: AsRef<str>,
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
