use proc_macro2::TokenStream;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_filter};

pub mod string;

#[derive(Debug)]
pub struct TransformParams<'a> {
    pub update_fields: FieldsParam<'a>,
}

pub trait TransformSpec {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForPassThrough<R>,
        update_fields: &[ValidFieldName],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()>;

    fn update_field(&self, src: TokenStream) -> TokenStream;
}

#[derive(Getters)]
pub struct Transform<Spec: TransformSpec> {
    spec: Spec,
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    update_fields: Vec<ValidFieldName>,
}

impl<Spec: TransformSpec> Transform<Spec> {
    pub fn new<R: TypeResolver + Copy>(
        spec: Spec,
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: TransformParams,
        trace: Trace,
        trace_name: &str,
    ) -> ChainResult<Self> {
        let valid_update_fields =
            params
                .update_fields
                .validate_on_stream(&inputs[0], graph, || trace_filter!(trace, trace_name))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .pass_through(|output_stream, facts_proof| {
                spec.update_facts(output_stream, &valid_update_fields, facts_proof)
            });
        let outputs = streams.build();
        Ok(Self {
            spec,
            name,
            inputs,
            outputs,
            update_fields: valid_update_fields,
        })
    }
}

impl<Spec: TransformSpec> DynNode for Transform<Spec> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let updates = {
            let mut_fields = self.update_fields.iter().map(ValidFieldName::mut_ident);

            let update_ops = self.update_fields.iter().map(|field| {
                let ident = field.ident();
                self.spec.update_field(quote! { record.#ident() })
            });

            quote! {
                #[allow(clippy::useless_conversion)]
                {
                    #(
                        *record.#mut_fields() = #update_ops;
                    )*
                }
            }
        };

        let inline_body = quote! {
            input.map(|mut record| {
                #updates
                Ok(record)
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}
