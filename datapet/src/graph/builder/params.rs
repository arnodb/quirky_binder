use ron::error::SpannedResult;
use serde::de;

pub struct ParamsBuilder {
    ron_options: ron::Options,
}

impl Default for ParamsBuilder {
    fn default() -> Self {
        Self {
            ron_options: ron::Options::default()
                .with_default_extension(ron::extensions::Extensions::UNWRAP_NEWTYPES),
        }
    }
}

impl ParamsBuilder {
    pub fn from_ron_str<'a, T>(&self, s: &'a str) -> SpannedResult<T>
    where
        T: de::Deserialize<'a>,
    {
        self.ron_options.from_str(s)
    }
}
