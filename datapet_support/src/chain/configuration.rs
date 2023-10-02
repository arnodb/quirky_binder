use std::collections::BTreeMap;

pub struct ChainConfiguration {
    pub variables: BTreeMap<String, String>,
}

impl ChainConfiguration {
    pub fn new() -> Self {
        Self {
            variables: BTreeMap::new(),
        }
    }
}

impl Default for ChainConfiguration {
    fn default() -> Self {
        Self::new()
    }
}
