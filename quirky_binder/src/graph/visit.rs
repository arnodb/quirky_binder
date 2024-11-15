use super::{DynNode, Graph};

pub trait Visit<'a> {
    fn visit_graph(&mut self, graph: &'a Graph) {
        visit_graph(self, graph)
    }

    fn visit_node(&mut self, node: &'a dyn DynNode) {
        visit_node(self, node)
    }
}

pub fn visit_graph<'a, V>(v: &mut V, graph: &'a Graph)
where
    V: Visit<'a> + ?Sized,
{
    for node in graph.entry_nodes.iter() {
        v.visit_node(node.as_ref())
    }
}

pub fn visit_node<'a, V>(v: &mut V, node: &'a dyn DynNode)
where
    V: Visit<'a> + ?Sized,
{
    for child in node.nodes() {
        v.visit_node(child);
    }
}
