use std::{
    collections::{btree_map::Entry, BTreeMap},
    path::PathBuf,
    str::FromStr,
};

use anyhow::Context;
use rustdoc_types::{Crate, Item, ItemEnum, Struct, StructKind};

fn main() -> Result<(), anyhow::Error> {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR is not set")?;
    let out_dir = std::env::var("OUT_DIR").context("OUT_DIR is not set")?;
    let mut json_path = PathBuf::from_str(&manifest_dir)
        .with_context(|| format!("could not parse CARGO_MANIFEST_DIR {}", manifest_dir))?;
    json_path.push("..");
    json_path.push("..");
    json_path.push("target");
    json_path.push("doc");
    json_path.push("quirky_binder_tests.json");
    println!("cargo:rerun-if-changed={}", json_path.to_string_lossy());
    let json = std::fs::read_to_string(&json_path)
        .with_context(|| format!("could not open JSON file {}", json_path.to_string_lossy()))?;
    let krate: Crate = serde_json::from_str(&json).context("could not parse JSON file")?;

    let mut all_threads = BTreeMap::<&[String], BTreeMap<usize, Vec<Option<&String>>>>::new();

    for item in krate.index.values() {
        if let Some(ThreadStatus {
            path,
            thread_num,
            nodes,
        }) = match_thread_status(item, &krate)?
        {
            match all_threads.entry(path).or_default().entry(thread_num) {
                Entry::Vacant(vacant) => {
                    vacant.insert(nodes);
                }
                Entry::Occupied(_) => {
                    panic!("Duplicate status {} thread {}", path.join("::"), thread_num);
                }
            }
        }
    }

    let mut actual_dir = PathBuf::from_str(&out_dir)
        .with_context(|| format!("could not parse OUT_DIR {}", manifest_dir))?;
    actual_dir.push("actual");
    if actual_dir.is_dir() {
        std::fs::remove_dir_all(&actual_dir).with_context(|| {
            format!(
                "could not clear actual directory {}",
                actual_dir.to_string_lossy()
            )
        })?;
    }

    for (script_path, threads) in all_threads {
        let mut path = actual_dir.clone();
        for dir in &script_path[..script_path.len() - 1] {
            path.push(dir);
        }
        std::fs::create_dir_all(&path).context("could not create actual target directory")?;
        path.push(format!("{}.json", script_path.last().unwrap()));
        let content = serde_json::to_string_pretty(&threads)
            .context("could not serialize threads to JSON")?;
        std::fs::write(path, content).context("could not write threads")?;
    }

    Ok(())
}

struct ThreadStatus<'a> {
    path: &'a [String],
    thread_num: usize,
    nodes: Vec<Option<&'a String>>,
}

fn match_thread_status<'a>(
    item: &'a Item,
    krate: &'a Crate,
) -> Result<Option<ThreadStatus<'a>>, anyhow::Error> {
    Ok(match &item.inner {
        ItemEnum::Struct(Struct {
            kind:
                StructKind::Plain {
                    fields,
                    has_stripped_fields: _,
                },
            generics: _,
            impls: _,
        }) => {
            if let Some(name) = &item.name {
                if name == "ThreadStatus" {
                    let path = &krate.paths[&item.id];
                    let path_len = path.path.len();
                    if path_len >= 4
                        && path.path[0] == "quirky_binder_tests"
                        && path.path[1] == "all_chains"
                    {
                        if let Some(thread_num_str) =
                            path.path[path_len - 2].strip_prefix("thread_")
                        {
                            let thread_num =
                                thread_num_str.parse::<usize>().with_context(|| {
                                    format!(
                                        "could not parse thread module {}",
                                        path.path[path_len - 2]
                                    )
                                })?;
                            let nodes = fields
                                .iter()
                                .map(|field_id| krate.index[field_id].docs.as_ref())
                                .collect();
                            Some(ThreadStatus {
                                path: &path.path[2..path_len - 2],
                                thread_num,
                                nodes,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    })
}
