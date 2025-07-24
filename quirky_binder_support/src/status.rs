/// Node state
pub enum NodeState {
    /// Everything is fine
    Good,
    /// An error occurred in the node
    Error(String),
}

/// Node status container
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
