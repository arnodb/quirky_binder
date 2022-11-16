use datapet_codegen::dtpt_mod_crate;

dtpt_mod_crate! {
    r#"
use crate::{
    filter::{
        anchor::anchorize,
        fork::extract_fields::extract_fields,
        group::group,
        in_place::string::{reverse_chars, to_lowercase},
        sort::sort,
    },
};

[] build_rev_table(token_field, reference_field) [reversed]
{
  (
    - extract_fields#extract_token(&[token_field, reference_field]) [extracted]
    -
  )

  ( < extracted
    - reverse_chars#reverse_token(&[token_field])
    - sort#sort_token(&[token_field])
    -> reversed
  )
}

[] build_sim_table(token_field, reference_field, ref_rs_field) [simplified]
{
  (
    - extract_fields#extract_token(&[token_field, reference_field]) [extracted]
    -
  )

  ( < extracted
    - to_lowercase#simplify_token(&[token_field])
    - sort#sort_token(&[token_field, reference_field])
    - group#group(&[reference_field], ref_rs_field)
    -> simplified
  )
}

pub [] build_word_list(token_field, anchor_field, sim_anchor_field, sim_rs_field) [rev_token, sim_token, rev_sim_token]
{
  (
    - build_sim_table#sim(token_field, anchor_field, sim_rs_field) [simplified]
    - build_rev_table#rev(token_field, anchor_field) [rev_token]
    -
  )

  ( < simplified
    - anchorize#anchorize(sim_anchor_field)
    - build_rev_table#sim_rev(token_field, sim_anchor_field) [rev_sim_token]
    -> sim_token
  )
}
"#
}
