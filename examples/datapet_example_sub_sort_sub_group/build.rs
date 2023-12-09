use datapet::dtpt;
use datapet::prelude::*;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    dtpt!(inline(
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
        fields: [("num_1", "u8"), ("num_2", "u8"), ("num_3", "u8"), ("num_4", "u8")],
        function: r#"{
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
        }"#,
      )
    - sort(fields: ["num_1"])
    - group(fields: ["num_2", "num_3", "num_4"], group_field: "nums_234")
    - sub_sort(path_fields: ["nums_234"], fields: ["num_2"])
    - sub_group(path_fields: ["nums_234"], fields: ["num_3", "num_4"], group_field: "nums_34")
    - sub_sort(path_fields: ["nums_234", "nums_34"], fields: ["num_3"])
    - sub_group(path_fields: ["nums_234", "nums_34"], fields: ["num_4"], group_field: "nums_4")
    - sub_sort(path_fields: ["nums_234", "nums_34", "nums_4"], fields: ["num_4"])
    - sub_group(path_fields: ["nums_234", "nums_34", "nums_4"], fields: [], group_field: "nums_4")
    - sink(
        debug: Some(r#"
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
        "#)
      )
  )
}
"###
    ));

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
