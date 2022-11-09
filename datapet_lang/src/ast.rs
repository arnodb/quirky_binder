#[derive(Debug)]
pub struct Module {
    pub graph_definitions: Vec<GraphDefinition>,
}

#[derive(Debug)]
pub struct GraphDefinition {
    pub signature: GraphDefinitionSignature,
    pub stream_lines: Vec<StreamLine>,
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
    pub params: Vec<FilterParam>,
    pub extra_outputs: Vec<String>,
}

#[derive(Debug)]
pub enum FilterParam {
    Single(String),
    Array(Vec<String>),
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
