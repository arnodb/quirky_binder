#[derive(Debug)]
pub struct Module<'a> {
    pub items: Vec<ModuleItem<'a>>,
}

#[derive(Debug)]
pub enum ModuleItem<'a> {
    UseDeclaration(UseDeclaration<'a>),
    GraphDefinition(GraphDefinition<'a>),
    Graph(Graph<'a>),
}

impl<'a> From<UseDeclaration<'a>> for ModuleItem<'a> {
    fn from(value: UseDeclaration<'a>) -> Self {
        ModuleItem::UseDeclaration(value)
    }
}

impl<'a> From<GraphDefinition<'a>> for ModuleItem<'a> {
    fn from(value: GraphDefinition<'a>) -> Self {
        ModuleItem::GraphDefinition(value)
    }
}

impl<'a> From<Graph<'a>> for ModuleItem<'a> {
    fn from(value: Graph<'a>) -> Self {
        ModuleItem::Graph(value)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct UseDeclaration<'a> {
    pub use_tree: &'a str,
}

#[derive(Debug)]
pub struct GraphDefinition<'a> {
    pub signature: GraphDefinitionSignature<'a>,
    pub stream_lines: Vec<StreamLine<'a>>,
    pub visible: bool,
}

#[derive(PartialEq, Eq, Debug)]
pub struct GraphDefinitionSignature<'a> {
    pub inputs: Option<Vec<&'a str>>,
    pub name: &'a str,
    pub params: Vec<&'a str>,
    pub outputs: Option<Vec<&'a str>>,
}

#[derive(Debug)]
pub struct StreamLine<'a> {
    pub filters: Vec<ConnectedFilter<'a>>,
    pub output: Option<StreamLineOutput<'a>>,
}

#[derive(Debug)]
pub struct ConnectedFilter<'a> {
    pub inputs: Vec<StreamLineInput<'a>>,
    pub filter: Filter<'a>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Filter<'a> {
    pub name: &'a str,
    pub alias: Option<&'a str>,
    pub params: &'a str,
    pub extra_outputs: Vec<&'a str>,
}

#[derive(Debug)]
pub enum StreamLineInput<'a> {
    Main(&'a str),
    Named(&'a str),
}

#[derive(Debug)]
pub enum StreamLineOutput<'a> {
    Main(&'a str),
    Named(&'a str),
}

#[derive(Debug)]
pub struct Graph<'a> {
    pub stream_lines: Vec<StreamLine<'a>>,
}
