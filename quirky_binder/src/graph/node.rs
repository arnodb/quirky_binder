use std::{
    collections::hash_map::DefaultHasher,
    fmt::Debug,
    hash::{Hash, Hasher},
};

use crate::prelude::*;

pub trait DynNode: Debug {
    fn name(&self) -> &FullyQualifiedName;

    fn inputs(&self) -> &[NodeStream];

    fn outputs(&self) -> &[NodeStream];

    fn identifier_for(&self, base: &str) -> syn::Ident {
        let mut hasher = DefaultHasher::new();
        self.name().to_string().hash(&mut hasher);
        let hash = hasher.finish();
        format_ident!("{base}_{hash:x}")
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain);

    fn nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(std::iter::empty())
    }

    fn is_cluster(&self) -> bool {
        false
    }
}

#[derive(Debug, new)]
pub struct NodeCluster<const IN: usize, const OUT: usize> {
    name: FullyQualifiedName,
    ordered_nodes: Vec<Box<dyn DynNode>>,
    inputs: [NodeStream; IN],
    outputs: [NodeStream; OUT],
}

impl<const IN: usize, const OUT: usize> NodeCluster<IN, OUT> {
    pub fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    pub fn inputs(&self) -> &[NodeStream; IN] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[NodeStream; OUT] {
        &self.outputs
    }
}

impl<const IN: usize, const OUT: usize> DynNode for NodeCluster<IN, OUT> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        for node in &self.ordered_nodes {
            node.gen_chain(graph, chain);
        }
    }

    fn nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(self.ordered_nodes.iter().map(|node| node.as_ref()))
    }

    fn is_cluster(&self) -> bool {
        true
    }
}
