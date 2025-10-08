@0x9798d4888e5d2770;

interface State {
    graph @0 () -> (graph :Graph);
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
