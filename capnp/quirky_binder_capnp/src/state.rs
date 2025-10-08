use capnp::capability::Promise;
use quirky_binder_support::prelude::*;

use crate::quirky_binder_capnp;

pub struct StateServer<T, U>
where
    T: DynChainStatus,
    U: AsRef<T>,
{
    chain_status: U,
    _t: std::marker::PhantomData<T>,
}

impl<T, U> StateServer<T, U>
where
    T: DynChainStatus,
    U: AsRef<T>,
{
    pub fn new(chain_status: U) -> Self {
        Self {
            chain_status,
            _t: Default::default(),
        }
    }
}

impl<T, U> quirky_binder_capnp::state::Server for StateServer<T, U>
where
    T: DynChainStatus,
    U: AsRef<T>,
{
    fn graph(
        &mut self,
        _: quirky_binder_capnp::state::GraphParams,
        mut results: quirky_binder_capnp::state::GraphResults,
    ) -> ::capnp::capability::Promise<(), ::capnp::Error> {
        let mut graph = results.get().init_graph();

        let mut nodes = graph.reborrow().init_nodes(
            self.chain_status
                .as_ref()
                .nodes_length()
                .try_into()
                .unwrap(),
        );
        for (i, n) in self.chain_status.as_ref().nodes().enumerate() {
            let mut node = nodes.reborrow().get(i.try_into().unwrap());
            node.set_name(n.name);
        }

        let mut edges = graph.reborrow().init_edges(
            self.chain_status
                .as_ref()
                .edges_length()
                .try_into()
                .unwrap(),
        );
        for (i, e) in self.chain_status.as_ref().edges().enumerate() {
            let mut edge = edges.reborrow().get(i.try_into().unwrap());
            edge.set_tail_name(e.tail_name);
            edge.set_tail_index(e.tail_index.try_into().unwrap());
            edge.set_head_name(e.head_name);
            edge.set_head_index(e.head_index.try_into().unwrap());
        }

        Promise::ok(())
    }
}
