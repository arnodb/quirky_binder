use crate::{
    stream::{RecordDefinitionFragments, RecordDefinitionFragmentsInfo},
    support::name::FullyQualifiedName,
};

pub const DEFAULT_CHAIN_ROOT_MODULE_NAME: [&str; 2] = ["crate", "chain"];
pub const DEFAULT_CHAIN_STREAMS_MODULE_NAME: &str = "streams";
pub const DEFAULT_CHAIN_ERROR_TYPE: [&str; 2] = ["anyhow", "Error"];
pub const DEFAULT_CHAIN_ERROR_MACRO: [&str; 2] = ["anyhow", "anyhow"];
pub const DEFAULT_CHAIN_MAIN_NAME: &str = "main";

#[derive(Debug)]
pub struct ChainCustomizer {
    pub streams_module_name: FullyQualifiedName,
    pub module_name: FullyQualifiedName,
    pub custom_module_imports: Vec<(String, String)>,
    pub error_type: FullyQualifiedName,
    pub error_macro: FullyQualifiedName,
    pub main_name: String,
    pub main_attrs: Vec<String>,
    pub threads: ThreadsCustomizer,
}

impl Default for ChainCustomizer {
    fn default() -> Self {
        Self {
            streams_module_name: FullyQualifiedName::new_n(
                DEFAULT_CHAIN_ROOT_MODULE_NAME
                    .iter()
                    .chain([DEFAULT_CHAIN_STREAMS_MODULE_NAME].iter()),
            ),
            module_name: FullyQualifiedName::new_n(DEFAULT_CHAIN_ROOT_MODULE_NAME.iter()),
            custom_module_imports: vec![],
            error_type: FullyQualifiedName::new_n(DEFAULT_CHAIN_ERROR_TYPE.iter()),
            error_macro: FullyQualifiedName::new_n(DEFAULT_CHAIN_ERROR_MACRO.iter()),
            main_name: DEFAULT_CHAIN_MAIN_NAME.to_owned(),
            main_attrs: Vec::default(),
            threads: ThreadsCustomizer::default(),
        }
    }
}

impl ChainCustomizer {
    pub fn definition_fragments<'c, I>(&'c self, info: &'c I) -> RecordDefinitionFragments<'c>
    where
        I: RecordDefinitionFragmentsInfo,
    {
        RecordDefinitionFragments::new(
            info.record_type(),
            info.variant_id(),
            &self.streams_module_name,
        )
    }
}

#[derive(Debug)]
pub struct ThreadsCustomizer {
    pub spawn: String,
}

pub const DEFAULT_THREADS_SPAWN: &str = "std::thread::spawn";

impl Default for ThreadsCustomizer {
    fn default() -> Self {
        Self {
            spawn: DEFAULT_THREADS_SPAWN.to_owned(),
        }
    }
}
