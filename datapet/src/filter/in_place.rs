use proc_macro2::TokenStream;
use truc::record::{
    definition::{RecordDefinition, RecordVariant},
    type_resolver::TypeResolver,
};

use crate::prelude::*;

#[derive(Getters)]
struct InPlaceFilter {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl InPlaceFilter {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        break_order_fact_at: &[&str],
        break_distinct_fact_for: Option<&[&str]>,
        _trace: Trace,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .pass_through(|output_stream, facts_proof| {
                output_stream.break_order_fact_at(break_order_fact_at);
                if let Some(break_distinct_fact_for) = break_distinct_fact_for {
                    output_stream.break_distinct_fact_for(break_distinct_fact_for);
                }
                facts_proof.order_facts_updated().distinct_facts_updated()
            });
        let outputs = streams.build();
        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }

    fn gen_chain<B>(&self, node: &dyn DynNode, graph: &Graph, chain: &mut Chain, body: B)
    where
        B: FnOnce(&RecordDefinition, &RecordVariant) -> TokenStream,
    {
        let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
        let record_variant = &record_definition[self.inputs.single().variant_id()];
        let body = body(record_definition, record_variant);

        let inline_body = quote! {
            input.map(|mut record| {
                #body
                Ok(record)
            })
        };

        chain.implement_inline_node(
            node,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn gen_chain_simple<'f, F>(
        &self,
        node: &dyn DynNode,
        graph: &Graph,
        chain: &mut Chain,
        fields: F,
        transform: TokenStream,
    ) where
        F: IntoIterator<Item = &'f str> + Clone,
    {
        self.gen_chain(node, graph, chain, |record_definition, variant| {
            let data = variant
                .data()
                .filter_map(|d| {
                    let datum = &record_definition[d];
                    if fields
                        .clone()
                        .into_iter()
                        .any(|field| field == datum.name())
                    {
                        Some(datum)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let mut_fields = data
                .iter()
                .map(|datum| format_ident!("{}_mut", datum.name()));
            let fields = data.iter().map(|datum| format_ident!("{}", datum.name()));
            quote! {
                #[allow(clippy::useless_conversion)]
                {
                    #(
                        *record.#mut_fields() = record.#fields()#transform.into();
                    )*
                }
            }
        });
    }
}

pub mod string {
    use std::ops::Deref;

    use serde::Deserialize;
    use truc::record::type_resolver::TypeResolver;

    use super::InPlaceFilter;
    use crate::{prelude::*, trace_filter};

    #[derive(Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    pub struct InplaceStringParams<'a> {
        #[serde(borrow)]
        fields: FieldsParam<'a>,
    }

    const TO_LOWERCASE_TRACE_NAME: &str = "to_lowercase";

    pub struct ToLowercase {
        in_place: InPlaceFilter,
        fields: Box<[Box<str>]>,
    }

    impl ToLowercase {
        pub fn inputs(&self) -> &[NodeStream; 1] {
            self.in_place.inputs()
        }

        pub fn outputs(&self) -> &[NodeStream; 1] {
            self.in_place.outputs()
        }
    }

    impl DynNode for ToLowercase {
        fn name(&self) -> &FullyQualifiedName {
            &self.in_place.name
        }

        fn inputs(&self) -> &[NodeStream] {
            &self.in_place.inputs
        }

        fn outputs(&self) -> &[NodeStream] {
            &self.in_place.outputs
        }

        fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
            self.in_place.gen_chain_simple(
                self,
                graph,
                chain,
                self.fields.iter().map(Box::as_ref),
                quote! { .to_lowercase() },
            );
        }

        fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
            Box::new(Some(self as &dyn DynNode).into_iter())
        }
    }

    pub fn to_lowercase<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: InplaceStringParams,
        trace: Trace,
    ) -> ChainResult<ToLowercase> {
        let valid_fields = params.fields.validate_on_stream(&inputs[0], graph, || {
            trace_filter!(trace, TO_LOWERCASE_TRACE_NAME)
        })?;
        Ok(ToLowercase {
            in_place: InPlaceFilter::new(
                graph,
                name,
                inputs,
                &valid_fields,
                Some(&valid_fields),
                trace,
            )?,
            fields: valid_fields
                .iter()
                .map(Deref::deref)
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    const REVERSE_CHARS_TRACE_NAME: &str = "reverse_chars";

    pub struct ReverseChars {
        in_place: InPlaceFilter,
        fields: Box<[Box<str>]>,
    }

    impl ReverseChars {
        pub fn inputs(&self) -> &[NodeStream; 1] {
            self.in_place.inputs()
        }

        pub fn outputs(&self) -> &[NodeStream; 1] {
            self.in_place.outputs()
        }
    }

    impl DynNode for ReverseChars {
        fn name(&self) -> &FullyQualifiedName {
            &self.in_place.name
        }

        fn inputs(&self) -> &[NodeStream] {
            &self.in_place.inputs
        }

        fn outputs(&self) -> &[NodeStream] {
            &self.in_place.outputs
        }

        fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
            self.in_place.gen_chain_simple(
                self,
                graph,
                chain,
                self.fields.iter().map(Box::as_ref),
                quote! { .chars().rev().collect::<String>() },
            );
        }

        fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
            Box::new(Some(self as &dyn DynNode).into_iter())
        }
    }

    pub fn reverse_chars<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: InplaceStringParams,
        trace: Trace,
    ) -> ChainResult<ReverseChars> {
        let valid_fields = params.fields.validate_on_stream(&inputs[0], graph, || {
            trace_filter!(trace, REVERSE_CHARS_TRACE_NAME)
        })?;
        Ok(ReverseChars {
            in_place: InPlaceFilter::new(
                graph,
                name,
                inputs,
                /* TODO nice to have: change order direction */ &valid_fields,
                None,
                trace,
            )?,
            fields: valid_fields
                .iter()
                .map(Deref::deref)
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }
}
