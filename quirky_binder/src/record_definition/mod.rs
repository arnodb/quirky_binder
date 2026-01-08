//! Replication of `truc` record definition but `truc` agnostic in order to allow later static type
//! resolution.

use std::collections::BTreeMap;

use truc::record::{
    definition::{
        builder::{
            generic::GenericRecordDefinitionBuilder,
            native::{DatumDefinitionOverride, NativeRecordDefinitionBuilder},
        },
        convert::convert_record_definition,
        DatumDefinition, NativeDatumDetails, RecordDefinition, RecordVariantId,
    },
    type_resolver::TypeResolver,
};

use crate::{chain::customizer::ChainCustomizer, stream::StreamRecordType};

#[derive(Clone, Debug)]
pub enum QuirkyDatumType {
    Simple {
        type_name: String,
    },
    Vec {
        record_type: StreamRecordType,
        variant_id: RecordVariantId,
    },
}

pub type QuirkyRecordDefinitionBuilder = GenericRecordDefinitionBuilder<QuirkyDatumType>;

/// Converts to a record definition with rust types and provide a variants mapping.
pub(crate) fn quirky_to_rust_definition<R>(
    quirky_definition: &RecordDefinition<QuirkyDatumType>,
    chain_customizer: &ChainCustomizer,
    type_resolver: R,
) -> Result<
    (
        RecordDefinition<NativeDatumDetails>,
        BTreeMap<RecordVariantId, RecordVariantId>,
    ),
    String,
>
where
    R: TypeResolver,
{
    let mut rust = NativeRecordDefinitionBuilder::new(type_resolver);
    let variants_mapping = convert_record_definition(
        quirky_definition,
        |rust, datum: &DatumDefinition<QuirkyDatumType>| {
            let rust_datum_id = match datum.details() {
                QuirkyDatumType::Simple { type_name } => {
                    rust.add_dynamic_datum(datum.name(), type_name)?
                }
                QuirkyDatumType::Vec {
                    record_type,
                    variant_id,
                } => {
                    let module_name = chain_customizer.streams_module_name.sub_n(&***record_type);
                    rust.add_datum_override::<Vec<()>, _>(
                        datum.name(),
                        DatumDefinitionOverride {
                            type_name: Some(format!("Vec<{module_name}::Record{variant_id}>")),
                            size: None,
                            align: None,
                            allow_uninit: None,
                        },
                    )?
                }
            };
            Ok(rust_datum_id)
        },
        |rust, datum_id| rust.remove_datum(datum_id),
        |rust| rust.close_record_variant(),
        &mut rust,
    )?;
    Ok((rust.build(), variants_mapping))
}
