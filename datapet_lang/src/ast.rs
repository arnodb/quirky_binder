#[derive(Debug)]
pub struct Module {
    pub items: Vec<ModuleItem>,
}

#[derive(Debug)]
pub enum ModuleItem {
    UseDeclaration(UseDeclaration),
    GraphDefinition(GraphDefinition),
    Graph(Graph),
}

impl From<UseDeclaration> for ModuleItem {
    fn from(value: UseDeclaration) -> Self {
        ModuleItem::UseDeclaration(value)
    }
}

impl From<GraphDefinition> for ModuleItem {
    fn from(value: GraphDefinition) -> Self {
        ModuleItem::GraphDefinition(value)
    }
}

impl From<Graph> for ModuleItem {
    fn from(value: Graph) -> Self {
        ModuleItem::Graph(value)
    }
}

#[derive(Debug)]
pub struct UseDeclaration {
    pub use_tree: String,
}

#[derive(Debug)]
pub struct GraphDefinition {
    pub signature: GraphDefinitionSignature,
    pub stream_lines: Vec<StreamLine>,
    pub visible: bool,
}

#[derive(Debug)]
pub struct GraphDefinitionSignature {
    pub inputs: Option<Vec<String>>,
    pub name: String,
    pub params: Vec<String>,
    pub outputs: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct StreamLine {
    pub filters: Vec<ConnectedFilter>,
    pub output: Option<StreamLineOutput>,
}

#[derive(Debug)]
pub struct ConnectedFilter {
    pub inputs: Vec<StreamLineInput>,
    pub filter: Filter,
}

#[derive(Debug)]
pub struct Filter {
    pub name: String,
    pub alias: Option<String>,
    pub params: Vec<String>,
    pub extra_outputs: Vec<String>,
}

#[derive(Debug)]
pub enum StreamLineInput {
    Main,
    Named(String),
}

#[derive(Debug)]
pub enum StreamLineOutput {
    Main,
    Named(String),
}

#[derive(Debug)]
pub struct Graph {
    pub stream_lines: Vec<StreamLine>,
}
