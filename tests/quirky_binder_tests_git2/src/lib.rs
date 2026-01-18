#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
#[allow(clippy::too_many_arguments)]
mod all_chains {
    include!(concat!(env!("OUT_DIR"), "/all_chains.rs"));
}
