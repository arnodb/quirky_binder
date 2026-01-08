use crate::{
    chain::StreamCustomizer,
    stream::{NodeStream, NodeSubStream, RecordDefinitionFragments},
    support::name::FullyQualifiedName,
};

pub const DEFAULT_CHAIN_ROOT_MODULE_NAME: [&str; 2] = ["crate", "chain"];
pub const DEFAULT_CHAIN_STREAMS_MODULE_NAME: &str = "streams";
pub const DEFAULT_CHAIN_ERROR_TYPE: [&str; 2] = ["anyhow", "Error"];
pub const DEFAULT_CHAIN_ERROR_MACRO: [&str; 2] = ["anyhow", "anyhow"];
pub const DEFAULT_CHAIN_MAIN_NAME: &str = "main";

pub struct ChainCustomizer {
    pub streams_module_name: FullyQualifiedName,
    pub module_name: FullyQualifiedName,
    pub custom_module_imports: Vec<(String, String)>,
    pub error_type: FullyQualifiedName,
    pub error_macro: FullyQualifiedName,
    pub main_name: String,
    pub main_attrs: Vec<String>,
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
            main_name: DEFAULT_CHAIN_MAIN_NAME.to_string(),
            main_attrs: Vec::default(),
        }
    }
}

impl StreamCustomizer for ChainCustomizer {
    fn stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeStream,
    ) -> RecordDefinitionFragments<'c> {
        RecordDefinitionFragments::new(
            stream.record_type(),
            stream.variant_id(),
            &self.streams_module_name,
        )
    }

    fn sub_stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeSubStream,
    ) -> RecordDefinitionFragments<'c> {
        RecordDefinitionFragments::new(
            stream.record_type(),
            stream.variant_id(),
            &self.streams_module_name,
        )
    }
}
