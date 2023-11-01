use datapet_codegen::dtpt;

dtpt! {
    datapet_def r#"
use crate::{
    filter::{
        anchor::anchor,
        fork::extract_fields::extract_fields,
        group::group,
        in_place::string::{reverse_chars, to_lowercase},
        sort::sort,
    },
};

[] build_rev_table(token_field, reference_field) [reversed]
{
  (
    - extract_fields#extract_token(fields: ["{{token_field}}", "{{reference_field}}"]) [extracted]
    -
  )

  ( < extracted
    - reverse_chars#reverse_token(fields: ["{{token_field}}"])
    - sort#sort_token(fields: ["{{token_field}}"])
    -> reversed
  )
}

[] build_ci_table(token_field, reference_field, refs_field) [case_insensitive]
{
  (
    - extract_fields#extract_token(fields: ["{{token_field}}", "{{reference_field}}"]) [extracted]
    -
  )

  ( < extracted
    - to_lowercase#lowercase_token(fields: ["{{token_field}}"])
    - sort#sort_token(fields: ["{{token_field}}", "{{reference_field}}"])
    - group#group(fields: ["{{reference_field}}"], group_field: "{{refs_field}}")
    -> case_insensitive
  )
}

pub [] build_word_list(token_field, anchor_field, ci_anchor_field, ci_refs_field) [rev_token, ci_token, rev_ci_token]
{
  (
    - build_ci_table#case_insensitive("{{token_field}}", "{{anchor_field}}", "{{ci_refs_field}}") [case_insensitive]
    - build_rev_table#rev("{{token_field}}", "{{anchor_field}}") [rev_token]
    -
  )

  ( < case_insensitive
    - anchor#anchor(anchor_field: "{{ci_anchor_field}}")
    - build_rev_table#ci_rev("{{token_field}}", "{{ci_anchor_field}}") [rev_ci_token]
    -> ci_token
  )
}
"#
}
