#[test]
fn test_threads() {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
    };

    use itertools::{EitherOrBoth, Itertools};
    use pretty_assertions::assert_eq;
    use serde_json::Value;
    use walkdir::WalkDir;

    let out_dir = std::env::var("OUT_DIR").unwrap();

    let mut actual_dir = PathBuf::from_str(&out_dir).unwrap();
    actual_dir.push("actual");
    assert!(actual_dir.is_dir());

    let expected_dir = Path::new("expected");
    assert!(expected_dir.is_dir());

    let actual_walk = WalkDir::new(&actual_dir).min_depth(1).sort_by_file_name();
    let expected_walk = WalkDir::new(expected_dir).min_depth(1).sort_by_file_name();

    let take_snapshot = std::env::var("TAKE_SNAPSHOT").is_ok();

    for either in actual_walk.into_iter().map(Result::unwrap).merge_join_by(
        expected_walk.into_iter().map(Result::unwrap),
        |actual, expected| {
            let actual_short_name = actual.path().strip_prefix(&actual_dir).unwrap();
            let expected_short_name = expected.path().strip_prefix(expected_dir).unwrap();
            actual_short_name.cmp(expected_short_name)
        },
    ) {
        if let Some((
            short_name,
            (actual_content, actual_json),
            (mut expected_content, mut expected_json),
        )) = match either {
            EitherOrBoth::Left(actual) => {
                if actual.path().is_file() {
                    let actual_content = std::fs::read_to_string(actual.path()).unwrap();
                    let actual_json =
                        serde_json::from_str::<serde_json::Value>(&actual_content).unwrap();
                    Some((
                        actual.path().strip_prefix(&actual_dir).unwrap().to_owned(),
                        (actual_content, actual_json),
                        (String::new(), Value::Null),
                    ))
                } else {
                    None
                }
            }
            EitherOrBoth::Right(expected) => {
                if expected.path().is_file() {
                    let expected_content = std::fs::read_to_string(expected.path()).unwrap();
                    let expected_json =
                        serde_json::from_str::<serde_json::Value>(&expected_content).unwrap();
                    Some((
                        expected
                            .path()
                            .strip_prefix(expected_dir)
                            .unwrap()
                            .to_owned(),
                        (String::new(), Value::Null),
                        (expected_content, expected_json),
                    ))
                } else {
                    None
                }
            }
            EitherOrBoth::Both(actual, expected) => {
                assert_eq!(
                    actual.file_type(),
                    expected.file_type(),
                    "{}",
                    actual.path().to_string_lossy()
                );
                if actual.path().is_file() {
                    let actual_content = std::fs::read_to_string(actual.path()).unwrap();
                    let actual_json =
                        serde_json::from_str::<serde_json::Value>(&actual_content).unwrap();
                    let expected_content = std::fs::read_to_string(expected.path()).unwrap();
                    let expected_json =
                        serde_json::from_str::<serde_json::Value>(&expected_content).unwrap();
                    Some((
                        expected
                            .path()
                            .strip_prefix(expected_dir)
                            .unwrap()
                            .to_owned(),
                        (actual_content, actual_json),
                        (expected_content, expected_json),
                    ))
                } else {
                    None
                }
            }
        } {
            if take_snapshot {
                let expected = expected_dir.join(&short_name);
                std::fs::create_dir_all(expected.parent().unwrap()).unwrap();
                std::fs::write(&expected, &actual_content).unwrap();
                expected_content = std::fs::read_to_string(expected).unwrap();
                expected_json =
                    serde_json::from_str::<serde_json::Value>(&expected_content).unwrap();
            }
            if actual_json != expected_json {
                assert_eq!(
                    actual_content,
                    expected_content,
                    "{}",
                    expected_dir.join(short_name).to_string_lossy()
                );
            }
        }
    }
}
