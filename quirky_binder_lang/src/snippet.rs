use annotate_snippets::{
    display_list::FormatOptions,
    snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation},
};

// Copied from https://github.com/botika/yarte
fn lines_offsets(s: &str) -> Vec<usize> {
    let mut lines = vec![0];
    let mut prev = 0;
    while let Some(len) = s[prev..].find('\n') {
        prev += len + 1;
        lines.push(prev);
    }
    lines
}

// Inspired by from https://github.com/botika/yarte
fn slice_spans<'a>(input: &'a str, part: &'a str) -> (usize, (usize, usize), (usize, usize)) {
    let (lo, hi) = {
        let a = input.as_ptr();
        let b = part.as_ptr();
        if a <= b {
            (
                b as usize - a as usize,
                (b as usize - a as usize + part.len()).min(input.len()),
            )
        } else {
            panic!("not a part of input");
        }
    };

    let lines = lines_offsets(input);

    const CONTEXT: usize = 3;

    let lo_index = match lines.binary_search(&lo) {
        Ok(index) => index,
        Err(index) => index - 1,
    }
    .saturating_sub(CONTEXT);
    let lo_line = lines[lo_index];
    let hi_index = match lines.binary_search(&hi) {
        Ok(index) => index,
        Err(index) => index,
    };
    let hi_line = lines
        .get(hi_index + CONTEXT)
        .copied()
        .unwrap_or(input.len());
    (
        lo_index + 1,
        (lo_line, hi_line),
        (lo - lo_line, hi - lo_line),
    )
}

pub fn snippet_for_input_and_part<'a>(
    label: &'a str,
    input: &'a str,
    part: &'a str,
) -> Snippet<'a> {
    let (line_start, (lo_line, hi_line), (lo, hi)) = slice_spans(input, part);

    let slice = Slice {
        source: &input[lo_line..hi_line],
        line_start,
        origin: None,
        annotations: vec![SourceAnnotation {
            label,
            range: (lo, hi),
            annotation_type: AnnotationType::Error,
        }],
        fold: false,
    };

    Snippet {
        title: Some(Annotation {
            id: None,
            label: Some(label),
            annotation_type: AnnotationType::Error,
        }),
        footer: vec![],
        slices: vec![slice],
        opt: FormatOptions {
            // No color until https://github.com/rust-lang/rust-analyzer/issues/15443
            // has a proper fix.
            color: false,
            ..Default::default()
        },
    }
}
