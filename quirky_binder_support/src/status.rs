/// Node state
#[derive(Debug)]
pub enum NodeState {
    /// Everything is fine
    Good,
    /// An error occurred in the node
    Error(String),
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
            state: NodeState::Good,
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
