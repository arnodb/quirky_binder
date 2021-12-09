use crate::prelude::*;
use proc_macro2::TokenStream;

#[datapet_node(in = "-", out = "-")]
struct InPlaceFilter {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
}

impl InPlaceFilter {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain, body: TokenStream) {
        let thread = chain.get_thread_id_and_module_by_source(self.inputs[0].source(), &self.name);

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread.thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let record = def.record();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let fn_def = quote! {
                pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                    #input
                    input.map(|mut record| {
                        #body
                        Ok(record)
                    })
                }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

pub mod string {
    use super::InPlaceFilter;
    use crate::graph::{DynNode, GraphBuilder};
    use crate::support::FullyQualifiedName;
    use crate::{
        chain::Chain,
        graph::{Graph, Node},
        stream::NodeStream,
    };
    use proc_macro2::TokenStream;

    pub struct ToLowercase {
        in_place: InPlaceFilter,
        fields: Box<[Box<str>]>,
        string_to_type: Option<TokenStream>,
    }

    impl Node<1, 1> for ToLowercase {
        fn inputs(&self) -> &[NodeStream; 1] {
            &self.in_place.inputs
        }

        fn outputs(&self) -> &[NodeStream; 1] {
            &self.in_place.outputs
        }
    }

    impl DynNode for ToLowercase {
        fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
            let mut_fields = self
                .fields
                .iter()
                .map(|field| format_ident!("{}_mut", **field));
            let fields = self.fields.iter().map(|field| format_ident!("{}", **field));
            let string_to_type = &self.string_to_type;
            self.in_place.gen_chain(
                graph,
                chain,
                quote! {
                    #(*record.#mut_fields() = record.#fields().to_lowercase()#string_to_type;)*
                },
            );
        }
    }

    pub fn to_lowercase<I, F>(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: I,
    ) -> ToLowercase
    where
        I: IntoIterator<Item = F>,
        F: Into<Box<str>>,
    {
        ToLowercase {
            in_place: InPlaceFilter::new(graph, name, inputs),
            fields: fields
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            string_to_type: None,
        }
    }

    pub fn to_lowercase_boxed_str<I, F>(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: I,
    ) -> ToLowercase
    where
        I: IntoIterator<Item = F>,
        F: Into<Box<str>>,
    {
        ToLowercase {
            in_place: InPlaceFilter::new(graph, name, inputs),
            fields: fields
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            string_to_type: Some(quote! {.into_boxed_str()}),
        }
    }

    pub struct ReverseChars {
        in_place: InPlaceFilter,
        fields: Box<[Box<str>]>,
        string_to_type: Option<TokenStream>,
    }

    impl Node<1, 1> for ReverseChars {
        fn inputs(&self) -> &[NodeStream; 1] {
            &self.in_place.inputs
        }

        fn outputs(&self) -> &[NodeStream; 1] {
            &self.in_place.outputs
        }
    }

    impl DynNode for ReverseChars {
        fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
            let mut_fields = self
                .fields
                .iter()
                .map(|field| format_ident!("{}_mut", **field));
            let fields = self.fields.iter().map(|field| format_ident!("{}", **field));
            let string_to_type = &self.string_to_type;
            self.in_place.gen_chain(
                graph,
                chain,
                quote! {
                    #(*record.#mut_fields() = record.#fields().chars().rev().collect::<String>()#string_to_type;)*
                },
            );
        }
    }

    pub fn reverse_chars<I, F>(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: I,
    ) -> ReverseChars
    where
        I: IntoIterator<Item = F>,
        F: Into<Box<str>>,
    {
        ReverseChars {
            in_place: InPlaceFilter::new(graph, name, inputs),
            fields: fields
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            string_to_type: None,
        }
    }

    pub fn reverse_chars_boxed_str<I, F>(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: I,
    ) -> ReverseChars
    where
        I: IntoIterator<Item = F>,
        F: Into<Box<str>>,
    {
        ReverseChars {
            in_place: InPlaceFilter::new(graph, name, inputs),
            fields: fields
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            string_to_type: Some(quote! {.into_boxed_str()}),
        }
    }
}
