use std::collections::BTreeSet;

use quirky_binder::{prelude::*, trace_element};
use serde::Deserialize;

const WALK_COMMITS_TRACE_NAME: &str = "walk_commits";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct WalkCommitsParams<'a> {
    repository: &'a str,
    pushes: Option<Vec<&'a str>>,
    hides: Option<Vec<&'a str>>,
    fields: FieldsParam<'a>,
}

#[derive(Debug, Getters)]
pub struct WalkCommits {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    repository: String,
    pushes: Vec<String>,
    hides: Vec<String>,
    fields: Vec<ValidFieldName>,
}

impl WalkCommits {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: WalkCommitsParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields = params
            .fields
            .validate_new()
            .with_trace_element(trace_element!())?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_main_stream(graph)
            .with_trace_element(trace_element!())?;

        streams
            .new_main_output(graph)
            .with_trace_element(trace_element!())?
            .update(|output_stream, facts_proof| {
                let mut output_stream_def = output_stream.record_definition().borrow_mut();
                let mut seen = BTreeSet::<&str>::new();
                for field in valid_fields.iter() {
                    let name = field.name();
                    if seen.contains(name) {
                        return Err(ChainError::Other {
                            msg: format!("Duplicate field {name}"),
                        })
                        .with_trace_element(trace_element!());
                    }
                    let r#type = match name {
                        "id" => "String",
                        "author" => "String",
                        "author_name" => "Option<String>",
                        "author_email" => "Option<String>",
                        "committer" => "String",
                        "committer_name" => "Option<String>",
                        "committer_email" => "Option<String>",
                        "time_seconds" => "i64",
                        "time_offset_minutes" => "i32",
                        "message" => "Option<String>",
                        "summary" => "Option<String>",
                        "body" => "Option<String>",
                        _ => {
                            return Err(ChainError::Other {
                                msg: format!("Unknown Git field {name}"),
                            })
                            .with_trace_element(trace_element!());
                        }
                    };
                    output_stream_def
                        .add_datum(
                            name,
                            QuirkyDatumType::Simple {
                                type_name: r#type.to_owned(),
                            },
                        )
                        .map_err(|err| ChainError::Other { msg: err })
                        .with_trace_element(trace_element!())?;
                    seen.insert(name);
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            name,
            inputs,
            outputs,
            repository: params.repository.to_owned(),
            pushes: params
                .pushes
                .unwrap_or_default()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            hides: params
                .hides
                .unwrap_or_default()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            fields: valid_fields,
        })
    }
}

impl DynNode for WalkCommits {
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
        let thread_id = chain.new_threaded_source(
            &self.name,
            ChainThreadType::Regular,
            &self.inputs,
            &self.outputs,
        );

        let output = chain.format_thread_output(
            thread_id,
            0,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );

        let repository = &self.repository;

        let pushes = &self.pushes;
        let hides = &self.hides;

        let def = chain
            .customizer()
            .definition_fragments(self.outputs.single());
        let record = def.record();
        let unpacked_record = def.unpacked_record();

        let new_record_fields = self
            .fields
            .iter()
            .map(|field| {
                let name = field.name();
                let ident = field.ident();
                match name {
                    "id" => {
                        quote! { #ident: commit.id().to_string() }
                    }
                    "author" => {
                        quote! { #ident: commit.author().to_string() }
                    }
                    "author_name" => {
                        quote! { #ident: commit.author().name().map(str::to_owned) }
                    }
                    "author_email" => {
                        quote! { #ident: commit.author().email().map(str::to_owned) }
                    }
                    "committer" => {
                        quote! { #ident: commit.committer().to_string() }
                    }
                    "committer_name" => {
                        quote! { #ident: commit.committer().name().map(str::to_owned) }
                    }
                    "committer_email" => {
                        quote! { #ident: commit.committer().email().map(str::to_owned) }
                    }
                    "time_seconds" => {
                        quote! { #ident: commit.time().seconds() }
                    }
                    "time_offset_minutes" => {
                        quote! { #ident: commit.time().offset_minutes() }
                    }
                    "message" => {
                        quote! { #ident: commit.message().map(str::to_owned) }
                    }
                    "summary" => {
                        quote! { #ident: commit.summary().map(str::to_owned) }
                    }
                    "body" => {
                        quote! { #ident: commit.body().map(str::to_owned) }
                    }
                    _ => {
                        unreachable!("Unknown Git field {}", name);
                    }
                }
            })
            .collect::<Vec<_>>();
        let new_record_fields = quote!(#(#new_record_fields),*);

        let thread_body = quote! {
            let mut output = #output;
            move || {
                use anyhow::Context;
                use git2::Repository;

                let repository = Repository::open(#repository)
                    .with_context(|| format!("Could not open repository {}", #repository))?;

                let mut revwalk = repository.revwalk()?;
                #(revwalk.push(repository.revparse_single(#pushes)?.id())?;)*
                #(revwalk.hide(repository.revparse_single(#hides)?.id())?;)*

                for rev in revwalk {
                    let commit_id = rev?;
                    let commit = repository.find_commit(commit_id)?;
                    let record = #record::new(#unpacked_record { #new_record_fields });
                    output.send(Some(record))?;
                }
                output.send(None)?;

                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn walk_commits(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: WalkCommitsParams,
) -> ChainResultWithTrace<WalkCommits> {
    let _trace_name = TraceName::push(WALK_COMMITS_TRACE_NAME);
    WalkCommits::new(graph, name, inputs, params)
}
