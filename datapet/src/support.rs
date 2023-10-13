use itertools::zip_eq;
use std::{
    fmt::{Display, Formatter},
    ops::Deref,
};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct FullyQualifiedName(Box<[Box<str>]>);

/// Fully qualified name helper
impl FullyQualifiedName {
    /// # Example
    ///
    /// ```
    /// use datapet::support::FullyQualifiedName;
    ///
    /// let name = FullyQualifiedName::new("foo");
    /// assert_eq!(name.to_string(), "foo");
    /// ```
    pub fn new<S>(item: S) -> Self
    where
        S: ToString,
    {
        Self::new_n(Some(&item))
    }

    /// # Example
    ///
    /// ```
    /// use datapet::support::FullyQualifiedName;
    ///
    /// let name = FullyQualifiedName::new_n(&["foo", "bar"]);
    /// assert_eq!(name.to_string(), "foo::bar");
    /// ```
    pub fn new_n<'s, I, S>(items: I) -> Self
    where
        I: IntoIterator<Item = &'s S>,
        S: ToString + 's,
    {
        Self(
            items
                .into_iter()
                .map(ToString::to_string)
                .inspect(|item| {
                    if item.contains(':') {
                        panic!(": forbidden in name fragment");
                    }
                })
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    /// # Example
    ///
    /// ```
    /// use datapet::support::FullyQualifiedName;
    ///
    /// let name = FullyQualifiedName::new("foo");
    /// let sub = name.sub("bar");
    /// assert_eq!(sub.to_string(), "foo::bar");
    /// ```
    pub fn sub<S>(&self, item: S) -> Self
    where
        S: ToString,
    {
        self.sub_n(Some(&item))
    }

    /// # Example
    ///
    /// ```
    /// use datapet::support::FullyQualifiedName;
    ///
    /// let name = FullyQualifiedName::new("foo");
    /// let sub = name.sub_n(&["bar", "more"]);
    /// assert_eq!(sub.to_string(), "foo::bar::more");
    /// ```
    pub fn sub_n<'s, I, S>(&self, items: I) -> Self
    where
        I: IntoIterator<Item = &'s S>,
        S: ToString + 's,
    {
        Self(
            self.0
                .iter()
                .cloned()
                .chain(
                    items
                        .into_iter()
                        .map(ToString::to_string)
                        .inspect(|item| {
                            if item.contains(':') {
                                panic!(": forbidden in name fragment");
                            }
                        })
                        .map(String::into_boxed_str),
                )
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    pub fn to_name(&self) -> proc_macro2::Ident {
        format_ident!("{}", **self.last().expect("last"))
    }
}

impl Display for FullyQualifiedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (i, n) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("::")?;
            }
            f.write_str(n)?;
        }
        Ok(())
    }
}

impl Deref for FullyQualifiedName {
    type Target = Box<[Box<str>]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn fields_eq<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = FStr>,
    FStr: AsRef<str>,
{
    let eq = Some(quote! {|a: &#record_type, b: &#record_type|}.to_string())
        .into_iter()
        .chain(fields.into_iter().enumerate().map(|(i, field)| {
            let and = if i > 0 { " && " } else { "" };
            format!(
                "{and}a.{field}().eq(b.{field}())",
                and = and,
                field = field.as_ref()
            )
        }))
        .collect::<String>();
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
    let eq = Some(quote! {|a: &#record_type_a, b: &#record_type_b|}.to_string())
        .into_iter()
        .chain(
            zip_eq(fields_a, fields_b)
                .enumerate()
                .map(|(i, (field_a, field_b))| {
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
    syn::parse_str::<syn::Expr>(&eq).expect("eq")
}

pub fn fields_cmp<F, FStr>(record_type: &syn::Type, fields: F) -> syn::Expr
where
    F: IntoIterator<Item = FStr>,
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
            format!(
                "{then}a.{field}().cmp(b.{field}(){end_then})",
                then = then,
                end_then = end_then,
                field = field.as_ref()
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
