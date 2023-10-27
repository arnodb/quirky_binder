#[macro_use]
extern crate quote;

use datapet::prelude::*;
use datapet_codegen::dtpt_mod;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    dtpt_mod! {
        r###"
use datapet::{
    filter::{
        sink::sink,
        group::{group, sub_group},
        sort::{sort, sub_sort},
        source::function::function_source,
    },
};

{
  (
      function_source(
        &[("num_1", "u8"), ("num_2", "u8"), ("num_3", "u8"), ("num_4", "u8")], &[], &[],
        r#"{
        use rand_chacha::rand_core::{RngCore, SeedableRng};

        let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
        println!("Seed: {:02x?}", rng.get_seed());

        for _ in 0..1024 {
            let mut nums = [0; 4];
            rng.fill_bytes(&mut nums);

            let record = new_record(nums[0] % 7, nums[1] % 7, nums[2] % 7, nums[3] % 7);
            out.send(Some(record))?;
        }
        out.send(None)?;
        Ok(())
        }"#
      )
    - sort(&["num_1".asc()])
    - group(&["num_2", "num_3", "num_4"], "nums_234")
    - sub_sort(&["nums_234"], &["num_2".asc()])
    - sub_group(&["nums_234"], &["num_3", "num_4"], "nums_34")
    - sub_sort(&["nums_234", "nums_34"], &["num_3".asc()])
    - sub_group(&["nums_234", "nums_34"], &["num_4"], "nums_4")
    - sub_sort(&["nums_234", "nums_34", "nums_4"], &["num_4".asc()])
    - sub_group(&["nums_234", "nums_34", "nums_4"], &[], "nums_4")
    - sink(
        Some(quote! {
            println!("{}:", record.num_1());
            for record_234 in record.nums_234() {
                println!("    {}:", record_234.num_2());
                for record_34 in record_234.nums_34() {
                    println!("        {}:", record_34.num_3());
                    for record_4 in record_34.nums_4() {
                        println!("            {}", record_4.num_4());
                    }
                }
            }
        })
      )
  )
}
"###
    }

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver
    };

    let graph = dtpt_main(GraphBuilder::new(
        &type_resolver,
        ChainCustomizer::default(),
    ));

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
