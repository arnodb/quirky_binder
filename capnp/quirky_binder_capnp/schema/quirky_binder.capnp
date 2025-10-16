@0x9798d4888e5d2770;

interface State {
    graph @0 () -> (graph :Graph);
    nodeStatuses @1 () -> (statuses :List(NodeStatusItem));
}

struct Graph {
    nodes @0 :List(Node);
    edges @1 :List(Edge);
}

struct Node {
    name @0 :Text;
}

struct Edge {
    tailName @0 :Text;
    tailIndex @1 :UInt8;
    headName @2 :Text;
    headIndex @3 :UInt8;
}

struct NodeStatusItem {
    nodeName @0 :Text;
    state @1 :NodeState;
    inputRead @2 :List(UInt32);
    outputWritten @3 :List(UInt32);
}

struct NodeState {
    union {
        good @0 :Void;
        error @1 :Text;
    }
}
