#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ValidFieldName(String, syn::Ident);

impl ValidFieldName {
    pub fn name(&self) -> &str {
        &self.0
    }

    pub fn ident(&self) -> &syn::Ident {
        &self.1
    }

    pub fn mut_ident(&self) -> syn::Ident {
        format_ident!("{}_mut", self.name())
    }
}

impl TryFrom<&str> for ValidFieldName {
    type Error = ();

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        let ident = syn::parse_str::<syn::Ident>(name).map_err(|_| ())?;
        Ok(Self(name.to_owned(), ident))
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValidFieldType(String, syn::Type);

impl ValidFieldType {
    pub fn type_name(&self) -> &str {
        &self.0
    }

    pub fn r#type(&self) -> &syn::Type {
        &self.1
    }
}

impl TryFrom<&str> for ValidFieldType {
    type Error = ();

    fn try_from(type_name: &str) -> Result<Self, Self::Error> {
        let typ = syn::parse_str::<syn::Type>(type_name).map_err(|_| ())?;
        Ok(Self(type_name.to_owned(), typ))
    }
}
