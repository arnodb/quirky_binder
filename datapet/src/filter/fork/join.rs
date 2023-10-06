use crate::{prelude::*, stream::UniqueNodeStream, support::fields_cmp_ab};
use truc::record::type_resolver::TypeResolver;

#[derive(Getters)]
pub struct Join {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 2],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    primary_fields: Vec<String>,
    secondary_fields: Vec<String>,
    joined_fields: Vec<String>,
}

impl Join {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 2],
        primary_fields: &[&str],
        secondary_fields: &[&str],
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        let joined_fields = {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let mut output_stream_def = output_stream.borrow_mut();
            let secondary_stream_def = graph
                .get_stream(inputs[1].record_type())
                .expect("secondary stream definition")
                .borrow();
            let variant = &secondary_stream_def[inputs[1].variant_id()];

            let joined_fields = variant
                .data()
                .filter_map(|d| {
                    let datum = &secondary_stream_def[d];
                    if !secondary_fields.iter().any(|field| *field == datum.name()) {
                        output_stream_def.copy_datum(datum);
                        Some(datum.name().to_owned())
                    } else {
                        None
                    }
                })
                .collect::<Vec<String>>();

            joined_fields
        };

        let outputs = streams.build(graph);

        Self {
            name,
            inputs,
            outputs,
            primary_fields: primary_fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            secondary_fields: secondary_fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            joined_fields,
        }
    }
}

impl DynNode for Join {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

        let primary_input_def = chain.stream_definition_fragments(&self.inputs[0]);
        let secondary_input_def = chain.stream_definition_fragments(&self.inputs[1]);
        let output_def = chain.stream_definition_fragments(self.outputs.unique());

        let secondary_unpacked_record = secondary_input_def.unpacked_record();
        let record_and_unpacked_out = output_def.record_and_unpacked_out();
        let unpacked_record_in = output_def.unpacked_record_in();

        let cmp = fields_cmp_ab(
            &primary_input_def.record(),
            &self.primary_fields,
            &secondary_input_def.record(),
            &self.secondary_fields,
        );

        let (joined_fields, joined_fields_defaults) = {
            let names = self
                .joined_fields
                .iter()
                .map(|name| format_ident!("{}", name))
                .collect::<Vec<_>>();
            (
                quote!(#(#names),*),
                // Left join.
                // TODO: support inner join.
                quote!(#(#names: Default::default()),*),
            )
        };

        let thread_body = quote! {
            move || {
                use std::cmp::Ordering;

                let primary_rx = thread_control.input_0.take().expect("primary input");
                let secondary_rx = thread_control.input_1.take().expect("secondary input");
                let tx = thread_control.output_0.take().expect("output");

                let cmp = #cmp;

                let mut secondary_finished = false;
                let mut secondary_record = None;

                while let Some(primary_record) = primary_rx.recv()? {
                    let equal = loop {
                        if secondary_finished {
                            break false;
                        }
                        if let Some(secondary_record) = secondary_record.as_ref() {
                            match cmp(&primary_record, secondary_record) {
                                Ordering::Greater => {
                                    // TODO log
                                }
                                Ordering::Equal => {
                                    break true;
                                }
                                Ordering::Less => {
                                    break false;
                                }
                            }
                        }
                        secondary_record = secondary_rx.recv()?;
                        if secondary_record.is_none() {
                            secondary_finished = true;
                        }
                    };
                    if equal {
                        let secondary_record = secondary_record.take().expect("secondary_record");
                        let #secondary_unpacked_record{ #joined_fields, .. } = secondary_record.unpack();
                        let #record_and_unpacked_out { record } = #record_and_unpacked_out::from((primary_record, #unpacked_record_in { #joined_fields }));
                        tx.send(Some(record))?;
                    } else {
                        let #record_and_unpacked_out { record } = #record_and_unpacked_out::from((primary_record, #unpacked_record_in { #joined_fields_defaults }));
                        tx.send(Some(record))?;
                    }
                }

                if !secondary_finished {
                    while secondary_rx.recv()?.is_some() {}
                    // TODO log
                }

                tx.send(None)?;
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn join<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 2],
    primary_fields: &[&str],
    secondary_fields: &[&str],
) -> Join {
    Join::new(graph, name, inputs, primary_fields, secondary_fields)
}
