use capnp::capability::Promise;
use capnp_rpc::pry;
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

    fn node_statuses(
        &mut self,
        _: quirky_binder_capnp::state::NodeStatusesParams,
        mut results: quirky_binder_capnp::state::NodeStatusesResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let length: usize = self
            .chain_status
            .as_ref()
            .statuses()
            .map(|thread_status| thread_status.lock().expect("lock").node_statuses().count())
            .sum();

        let mut statuses = results.get().init_statuses(length.try_into().unwrap());

        let mut i = 0;
        for thread_status in self.chain_status.as_ref().statuses() {
            let thread_status = thread_status.lock().expect("lock");
            for s in thread_status.node_statuses() {
                let mut status = statuses.reborrow().get(i);

                status.set_node_name(s.node_name.to_owned());
                match s.state {
                    NodeState::Good => {
                        status.reborrow().init_state().set_good(());
                    }
                    NodeState::Error(err) => {
                        status.reborrow().init_state().set_error(err);
                    }
                }
                pry!(status.set_input_read(
                    &*s.input_read
                        .iter()
                        .copied()
                        .map(|a| u32::try_from(a).unwrap())
                        .collect::<Vec<_>>(),
                ));
                pry!(status.set_output_written(
                    &*s.output_written
                        .iter()
                        .copied()
                        .map(|a| u32::try_from(a).unwrap())
                        .collect::<Vec<_>>(),
                ));

                i += 1;
            }
        }

        Promise::ok(())
    }
}
