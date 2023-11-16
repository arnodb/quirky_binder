use std::borrow::Cow;

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
    pub use_tree: Cow<'a, str>,
}

#[derive(Debug)]
pub struct GraphDefinition<'a> {
    pub signature: GraphDefinitionSignature<'a>,
    pub stream_lines: Vec<StreamLine<'a>>,
    pub visible: bool,
}

#[derive(PartialEq, Eq, Debug)]
pub struct GraphDefinitionSignature<'a> {
    pub inputs: Option<Vec<Cow<'a, str>>>,
    pub name: Cow<'a, str>,
    pub params: Vec<Cow<'a, str>>,
    pub outputs: Option<Vec<Cow<'a, str>>>,
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

#[derive(Debug)]
pub struct Filter<'a> {
    pub name: Cow<'a, str>,
    pub alias: Option<Cow<'a, str>>,
    pub params: Cow<'a, str>,
    pub extra_outputs: Vec<Cow<'a, str>>,
}

#[derive(Debug)]
pub enum StreamLineInput<'a> {
    Main(Cow<'a, str>),
    Named(Cow<'a, str>),
}

#[derive(Debug)]
pub enum StreamLineOutput<'a> {
    Main(Cow<'a, str>),
    Named(Cow<'a, str>),
}

#[derive(Debug)]
pub struct Graph<'a> {
    pub stream_lines: Vec<StreamLine<'a>>,
}
