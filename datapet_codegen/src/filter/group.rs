use crate::{
    chain::{Chain, ImportScope},
    dyn_node,
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::{NodeStream, NodeStreamSource, StreamRecordType},
    support::{fields_eq, FullyQualifiedName},
};
use itertools::Itertools;
use truc::record::definition::DatumDefinitionOverride;

pub struct Group {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
    rs_stream: NodeStream,
    fields: Vec<String>,
    rs_field: String,
}

impl Node<1, 1> for Group {
    fn inputs(&self) -> &[NodeStream; 1] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream; 1] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
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

            let def_input =
                self.inputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let def_rs = self
                .rs_stream
                .definition_fragments(&graph.chain_customizer().streams_module_name);
            let input_unpacked_record = def_input.unpacked_record();
            let record = def.record();
            let unpacked_record_in = def.unpacked_record_in();
            let record_and_unpacked_out = def.record_and_unpacked_out();
            let rs_record = def_rs.record();
            let rs_unpacked_record = def_rs.unpacked_record();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let fields =
                syn::parse_str::<syn::Expr>(&self.fields.iter().join(", ")).expect("fields");

            let rs_field = format_ident!("{}", self.rs_field);
            let mut_rs_field = format_ident!("{}_mut", self.rs_field);

            let record_definition = &graph.record_definitions()[self.inputs[0].record_type()];
            let variant = record_definition
                .get_variant(self.inputs[0].variant_id())
                .unwrap_or_else(|| panic!("variant #{}", self.inputs[0].variant_id()));
            let eq = fields_eq(variant.data().filter_map(|d| {
                let datum = record_definition
                    .get_datum_definition(d)
                    .unwrap_or_else(|| panic!("datum #{}", d));
                if !self.fields.iter().any(|f| f == datum.name()) && datum.name() != self.rs_field {
                    Some(datum.name())
                } else {
                    None
                }
            }));

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                      #input
                      datapet_support::iterator::group::Group::new(
                          input,
                          |rec| {{
                              let #record_and_unpacked_out { mut record, #fields } = #record_and_unpacked_out::from((rec, #unpacked_record_in { #rs_field: Vec::new() }));
                              let rs_record = #rs_record::new(#rs_unpacked_record { #fields });
                              record.#mut_rs_field().push(rs_record);
                              record
                          }},
                          #eq,
                          |group, rec| {
                              let #input_unpacked_record{ #fields, .. } = rec.unpack();
                              let rs_record = #rs_record::new(#rs_unpacked_record { #fields });
                              group.#mut_rs_field().push(rs_record);
                          },
                      )
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

dyn_node!(Group);

pub fn group(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
    rs_field: &str,
) -> Group {
    let [input] = inputs;

    let rs_record_type = StreamRecordType::from(name.sub("rs"));
    graph.new_stream(rs_record_type.clone());

    let (variant_id, rs_stream) = {
        let mut stream = graph
            .get_stream(input.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, input.record_type()))
            .borrow_mut();

        let mut rs_stream = graph
            .get_stream(&rs_record_type)
            .unwrap_or_else(|| panic!(r#"stream "{}""#, rs_record_type))
            .borrow_mut();

        for &field in fields {
            let datum = stream
                .get_variant_datum_definition_by_name(input.variant_id(), field)
                .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
            rs_stream.copy_datum(datum);
            let datum_id = datum.id();
            stream.remove_datum(datum_id);
        }
        let rs_variant_id = rs_stream.close_record_variant();

        let module_name = graph
            .chain_customizer()
            .streams_module_name
            .sub_n(&**rs_record_type);
        stream.add_datum_override::<Vec<()>, _>(
            rs_field,
            DatumDefinitionOverride {
                type_name: Some(format!(
                    "Vec<{module_name}::Record{rs_variant_id}>",
                    module_name = module_name,
                    rs_variant_id = rs_variant_id,
                )),
                size: None,
                allow_uninit: None,
            },
        );
        (
            stream.close_record_variant(),
            NodeStream::new(rs_record_type, rs_variant_id, NodeStreamSource::default()),
        )
    };

    let record_type = input.record_type().clone();
    Group {
        name: name.clone(),
        inputs: [input],
        outputs: [NodeStream::new(
            record_type,
            variant_id,
            NodeStreamSource::from(name),
        )],
        rs_stream,
        fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
        rs_field: rs_field.to_string(),
    }
}
