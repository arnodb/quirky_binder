use crate::{prelude::*, support::fields_eq};
use itertools::Itertools;
use truc::record::definition::DatumDefinitionOverride;

#[derive(Getters)]
pub struct Group {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    rs_stream: NodeStream,
    fields: Vec<String>,
    rs_field: String,
}

impl Group {
    pub fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: &[&str],
        rs_field: &str,
    ) -> Group {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("rs", graph);

        let rs_stream = {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let rs_stream = output_stream.new_named_sub_stream("rs", graph);
            let input_variant_id = output_stream.input_variant_id();
            let mut output_stream_def = output_stream.borrow_mut();

            {
                let mut rs_stream_def = rs_stream.borrow_mut();
                for &field in fields {
                    let datum = output_stream_def
                        .get_variant_datum_definition_by_name(input_variant_id, field)
                        .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
                    rs_stream_def.copy_datum(datum);
                    let datum_id = datum.id();
                    output_stream_def.remove_datum(datum_id);
                }
            }
            let rs_stream = rs_stream.close_record_variant();

            let module_name = graph
                .chain_customizer()
                .streams_module_name
                .sub_n(&***rs_stream.record_type());
            output_stream_def.add_datum_override::<Vec<()>, _>(
                rs_field,
                DatumDefinitionOverride {
                    type_name: Some(format!(
                        "Vec<{module_name}::Record{rs_variant_id}>",
                        module_name = module_name,
                        rs_variant_id = rs_stream.variant_id(),
                    )),
                    size: None,
                    allow_uninit: None,
                },
            );

            rs_stream
        };

        let outputs = streams.build();

        Group {
            name: name.clone(),
            inputs,
            outputs,
            rs_stream,
            fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
            rs_field: rs_field.to_string(),
        }
    }
}

impl DynNode for Group {
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

pub fn group(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
    rs_field: &str,
) -> Group {
    Group::new(graph, name, inputs, fields, rs_field)
}
