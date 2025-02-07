use std::{collections::BTreeSet, fmt::Display};

#[derive(Default)]
pub struct Module {
    imports: BTreeSet<String>,
    modules: Vec<(String, Module)>,
    fragments: Vec<String>,
}

impl Module {
    pub fn import(&mut self, path: impl AsRef<str>, ty: impl AsRef<str>) {
        self.imports
            .insert(format!("{}::{}", path.as_ref(), ty.as_ref()));
    }

    pub fn get_module(&mut self, name: &str) -> Option<&mut Module> {
        self.modules
            .iter_mut()
            .find_map(|(n, m)| (n == name).then_some(m))
    }

    pub fn get_or_new_module(&mut self, name: &str) -> &mut Module {
        let pos = self.modules.iter_mut().position(|(n, _m)| n == name);
        if let Some(pos) = pos {
            &mut self.modules[pos].1
        } else {
            self.modules.push((name.to_owned(), Module::default()));
            &mut self.modules.last_mut().unwrap().1
        }
    }

    pub fn fragment(&mut self, fragment: impl Into<String>) {
        self.fragments.push(fragment.into());
    }
}

impl Display for Module {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.imports.is_empty() {
            for import in &self.imports {
                writeln!(f, "use {import};")?;
            }
            f.write_str("\n")?;
        }
        for (name, module) in &self.modules {
            writeln!(f, "pub mod {name} {{")?;
            module.fmt(f)?;
            writeln!(f, "}}")?;
            f.write_str("\n")?;
        }
        for fragment in &self.fragments {
            f.write_str(fragment)?;
            f.write_str("\n")?;
        }
        Ok(())
    }
}
