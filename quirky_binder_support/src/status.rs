use std::sync::{Arc, Mutex};

/// ChainStatus
pub trait DynChainStatus {
    fn nodes_length(&self) -> usize;

    fn nodes(&self) -> impl Iterator<Item = NodeDescriptor<'_>>;

    fn edges_length(&self) -> usize;

    fn edges(&self) -> impl Iterator<Item = EdgeDescriptor<'_>>;

    fn statuses(&self) -> impl Iterator<Item = Arc<Mutex<dyn DynThreadStatus>>>;
}

impl<T: DynChainStatus> DynChainStatus for &T {
    fn nodes_length(&self) -> usize {
        (*self).nodes_length()
    }

    fn nodes(&self) -> impl Iterator<Item = NodeDescriptor<'_>> {
        (*self).nodes()
    }

    fn edges_length(&self) -> usize {
        (*self).edges_length()
    }

    fn edges(&self) -> impl Iterator<Item = EdgeDescriptor<'_>> {
        (*self).edges()
    }

    fn statuses(&self) -> impl Iterator<Item = Arc<Mutex<dyn DynThreadStatus>>> {
        (*self).statuses()
    }
}

/// Node
pub struct NodeDescriptor<'a> {
    pub name: &'a str,
}

/// Edge
pub struct EdgeDescriptor<'a> {
    pub tail_name: &'a str,
    pub tail_index: usize,
    pub head_name: &'a str,
    pub head_index: usize,
}

/// Node state
#[derive(Debug)]
pub enum NodeState {
    /// The node is waiting for data to process.
    Waiting,
    /// The node is running.
    Running,
    /// The node successfully completed its processing.
    Success,
    /// An error occurred in the node.
    Error(String),
}

impl NodeState {
    pub fn transition_to(&mut self, new_state: Self, node_name: &str) {
        match (&self, &new_state) {
            // Running to Running is allowed when there are multiple inputs
            (Self::Running, Self::Waiting)
            | (Self::Success, Self::Waiting | Self::Running | Self::Success)
            | (Self::Error(_), Self::Waiting | Self::Running | Self::Success | Self::Error(_)) => {
                eprintln!("Should not transition from {self:?} to {new_state:?} in {node_name}");
            }
            (_, _) => {
                *self = new_state;
            }
        }
    }
}

/// Node status container
#[derive(Debug)]
pub struct NodeStatus<const I: usize, const O: usize> {
    /// The node state
    pub state: NodeState,
    /// The number of records read per input
    pub input_read: [usize; I],
    /// The number of records written per output
    pub output_written: [usize; O],
}

impl<const I: usize, const O: usize> Default for NodeStatus<I, O> {
    fn default() -> Self {
        Self {
            state: NodeState::Waiting,
            input_read: [0; I],
            output_written: [0; O],
        }
    }
}

#[derive(Debug)]
/// Item returned by [`DynThreadStatus::node_statuses`].
pub struct NodeStatusItem<'a> {
    pub node_name: &'a str,
    pub state: &'a NodeState,
    pub input_read: &'a [usize],
    pub output_written: &'a [usize],
}

/// Interface to access multi-node thread statuses.
pub trait DynThreadStatus: Send {
    fn node_statuses(&self) -> Box<dyn Iterator<Item = NodeStatusItem<'_>> + '_>;
}
