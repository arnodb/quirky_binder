use quirky_binder_codegen_macro::quirky_binder_internal;

quirky_binder_internal!(inline(
    r###"
use crate::{
    filter::{
        function::{
            execute::function_execute,
        },
    },
};

pub monitor()
{
  (
      function_execute(
        thread_type: Background,
        body: r#"
            use std::{
                fs::File,
                io::{stderr, Write},
                time::Duration,
            };

            use chrono::{DateTime, Utc};

            let mut csv = File::create("qb_monitor.csv").unwrap();
            writeln!(csv, "Timestamp,memory_rss,memory_swap,memory_object,memory_wrapped").unwrap();

            let mut meter = self_meter::Meter::new(Duration::from_millis(1000)).unwrap();
            meter.track_current_thread("main");

            loop {
                meter.scan()
                    .map_err(|e| writeln!(&mut stderr(), "Scan error: {}", e))
                    .ok();

                let report = meter.report();

                if let Some(report) = report.as_ref() {
                    let timestamp = DateTime::<Utc>::from(report.timestamp);
                    let data = crate::quirky_binder_monitor::DATA.lock().unwrap();
                    writeln!(
                        csv,
                        "{},{},{},{},{}",
                        timestamp,
                        report.memory_rss,
                        report.memory_swap,
                        data.object,
                        data.wrapped,
                    ).unwrap();
                }

                //println!("Report: {:#?}", report);

                {
                    let is_interrupted = thread_control.interrupt.0.lock().unwrap();
                    if *is_interrupted {
                        break;
                    }
                    let is_interrupted = thread_control.interrupt.1.wait_timeout(is_interrupted, Duration::from_millis(1000)).unwrap().0;
                    if *is_interrupted {
                        break;
                    }
                }
            }

            Ok(())
        "#,
      )
  )
}

"###
));
