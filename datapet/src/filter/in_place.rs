use proc_macro2::TokenStream;
use truc::record::type_resolver::TypeResolver;

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

    fn gen_chain_simple<'f, F>(
        &self,
        node: &dyn DynNode,
        chain: &mut Chain,
        fields: F,
        transform: TokenStream,
    ) where
        F: IntoIterator<Item = &'f ValidFieldName> + Clone,
    {
        let mut_fields = fields.clone().into_iter().map(ValidFieldName::mut_ident);
        let fields = fields.into_iter().map(ValidFieldName::ident);

        let inline_body = quote! {
            input.map(|mut record| {
                #[allow(clippy::useless_conversion)]
                {
                    #(
                        *record.#mut_fields() = record.#fields()#transform.into();
                    )*
                }
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
}

pub mod string {
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
        fields: Vec<ValidFieldName>,
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

        fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
            self.in_place
                .gen_chain_simple(self, chain, &self.fields, quote! { .to_lowercase() });
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
        let fields_for_facts = valid_fields
            .iter()
            .map(ValidFieldName::name)
            .collect::<Vec<_>>();
        Ok(ToLowercase {
            in_place: InPlaceFilter::new(
                graph,
                name,
                inputs,
                &fields_for_facts,
                Some(&fields_for_facts),
                trace,
            )?,
            fields: valid_fields,
        })
    }

    const REVERSE_CHARS_TRACE_NAME: &str = "reverse_chars";

    pub struct ReverseChars {
        in_place: InPlaceFilter,
        fields: Vec<ValidFieldName>,
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

        fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
            self.in_place.gen_chain_simple(
                self,
                chain,
                &self.fields,
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
        let fields_for_facts = valid_fields
            .iter()
            .map(ValidFieldName::name)
            .collect::<Vec<_>>();
        Ok(ReverseChars {
            in_place: InPlaceFilter::new(
                graph,
                name,
                inputs,
                /* TODO nice to have: change order direction */ &fields_for_facts,
                None,
                trace,
            )?,
            fields: valid_fields,
        })
    }
}
