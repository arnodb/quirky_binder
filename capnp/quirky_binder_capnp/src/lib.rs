capnp::generated_code!(pub mod quirky_binder_capnp);

pub mod state;

// Until further developments, quirky_binder declares that teleop is only supported on "linux".

#[cfg(target_os = "linux")]
pub use linux::{discover_processes, run_chain};
#[cfg(not(target_os = "linux"))]
pub use no_teleop::{discover_processes, run_chain};

#[derive(Clone, Debug)]
pub struct Process {
    pub pid: u32,
    pub description: String,
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        fs::File,
        path::PathBuf,
        pin::pin,
        str::FromStr,
        sync::{Arc, LazyLock},
        time::{Duration, Instant},
    };

    use futures::{task::LocalSpawnExt, AsyncReadExt, FutureExt, StreamExt};
    pub use nix::unistd::Pid;
    use nix::{
        sys::signal::kill,
        unistd::{getuid, User},
    };
    use quirky_binder_support::status::DynChainStatus;
    use smol::{lock::Mutex, net::unix::UnixStream, Timer};
    use smol_cancellation_token::CancellationToken;
    use teleop::{
        attach::unix_socket::listen,
        operate::capnp::{run_server_connection, teleop_capnp, TeleopServer},
    };

    use crate::{quirky_binder_capnp, state::StateServer, Process};

    fn process_directory() -> Result<PathBuf, Box<dyn std::error::Error>> {
        let uid = getuid();
        let user = User::from_uid(uid)?.ok_or("Unable to resolve current user")?;
        Ok(std::env::temp_dir().join(format!("quirky_binder_{}", user.name)))
    }

    fn reveal_process() -> Result<AutoDropFile, Box<dyn std::error::Error>> {
        let dir = process_directory()?;
        std::fs::create_dir_all(&dir)?;
        let pid = std::process::id();
        let file = dir.join(pid.to_string());
        Ok(AutoDropFile::create(file)?)
    }

    pub fn run_chain<S, J, E>(
        chain_status: S,
        join_chain: J,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        S: DynChainStatus + 'static,
        J: FnOnce() -> Result<(), E> + Send + 'static,
        E: Into<Box<dyn std::error::Error>> + Send + 'static,
        Box<dyn std::error::Error>: From<E>,
    {
        let mut exec = futures::executor::LocalPool::new();
        let spawn = exec.spawner();

        exec.run_until(async {
            let _reveal_process = reveal_process()?;

            // We need to know when the chain ends and to remember it.
            let (end_of_chain_sender, end_of_chain_receiver) = futures::channel::oneshot::channel();

            let join_main = std::thread::spawn(move || {
                let res = join_chain();
                let _ = end_of_chain_sender.send(());
                res
            });

            // We need to know whether or not at least 1 connection was opened and what is the
            // current number of open connections.
            // If it goes down to 0 then it is considered the end of teleoperations.
            let (mut count_sender, mut count_receiver) = async_broadcast::broadcast::<usize>(1);
            count_sender.set_overflow(true);

            // Signal that teleoperations should end.
            let teleop_token = CancellationToken::new();

            let mut chain_fut = pin!(
                // Wait for chain end
                end_of_chain_receiver
                    // Then join the thread to release resources
                    .then(async |res| -> Result<(), Box<dyn std::error::Error>> {
                        join_main
                            .join()
                            .map_err(|_err| "Unable to join main thread".to_owned())??;
                        // Joining the thread will fail first, this error handling is unreachable.
                        res.map_err(|futures::channel::oneshot::Canceled| {
                            "Main thread panicked".to_owned()
                        })?;
                        Ok(())
                    })
                    // Then wait for end of teleoperations with a maximum delay
                    .then(async |res| {
                        eprintln!("Waiting for end of teleoperations...");
                        // Note that the loop still accepts late connections just in case. That is
                        // why there is a timeout.
                        let mut deadline = FutureExt::fuse(Timer::at(
                            Instant::now() + Duration::from_millis(10000),
                        ));
                        loop {
                            futures::select! {
                                res = count_receiver.recv().fuse() => {
                                    match res {
                                        Ok(0) => {
                                            eprintln!("All teleop connections closed");
                                            break;
                                        }
                                        Ok(_) => { /* maybe next time */ }
                                        Err(err) => {
                                            eprintln!("Broadcast error {err}");
                                            break;
                                        }
                                    }
                                }
                                _ = deadline => {
                                    eprintln!("Timeout");
                                    break;
                                }
                            }
                        }
                        if !teleop_token.is_cancelled() {
                            teleop_token.cancel();
                        }
                        res
                    })
            );

            let client = LazyLock::new(|| {
                let mut server = TeleopServer::new();
                server
                    .register_service::<quirky_binder_capnp::state::Client, _, _>("state", || {
                        StateServer::new(Arc::new(chain_status))
                    });
                capnp_rpc::new_client::<teleop_capnp::teleop::Client, _>(server)
            });

            let connection_count = Arc::new(Mutex::new(0));

            // New connection handler, takes care of counting the connections and early stopping
            // teleoperations if at least one connection was opened and all connections are then
            // closed.
            let handle_new_connection = async |stream: UnixStream| {
                let count = {
                    let mut count = connection_count.lock().await;
                    (*count) += 1;
                    *count
                };
                let _ = count_sender.broadcast(count).await;
                if let Err(e) = spawn.spawn_local({
                    let count_sender = count_sender.clone();
                    let teleop_token = teleop_token.clone();
                    let connection_count = connection_count.clone();
                    let client = client.client.hook.clone();
                    async move {
                        let (input, output) = stream.split();
                        futures::select! {
                            res = run_server_connection(input, output, client).fuse() => {
                                if let Err(err) = res {
                                    eprintln!("Error while running server connection: {err}");
                                }
                            }
                            () = teleop_token.cancelled().fuse() => {}
                        }
                        let count = {
                            let mut count = connection_count.lock().await;
                            (*count) -= 1;
                            *count
                        };
                        let _ = count_sender.broadcast(count).await;
                    }
                }) {
                    eprintln!("Error while spawning connection handler: {e}");
                    let count = {
                        let mut count = connection_count.lock().await;
                        (*count) -= 1;
                        *count
                    };
                    let _ = count_sender.broadcast(count).await;
                }
            };

            let mut conn_stream = pin!(listen());

            loop {
                futures::select! {
                    stream = conn_stream.next().fuse() => {
                        if let Some(stream) = stream {
                            if teleop_token.is_cancelled(){
                                continue;
                            }
                            let (stream, _addr) = stream?;
                            handle_new_connection(stream).await;
                        } else {
                            break;
                        }
                    }
                    _ = chain_fut => {
                        break;
                    }
                }
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        })?;

        exec.run();

        Ok(())
    }

    pub fn discover_processes() -> Result<Vec<Process>, Box<dyn std::error::Error>> {
        let dir = process_directory()?;
        let entries = std::fs::read_dir(dir);
        Ok(entries
            .ok()
            .map(|entries| {
                entries
                    .filter_map(|entry| {
                        entry
                            .ok()
                            .filter(|entry| entry.path().is_file())
                            .and_then(|entry| {
                                let pid = u32::from_str(
                                    &entry.path().file_name().unwrap().to_string_lossy(),
                                )
                                .ok();
                                pid.map(|pid| {
                                    // Set the description to the executable name at this time.
                                    let mut path = PathBuf::new();
                                    path.push("/proc");
                                    path.push(pid.to_string());
                                    path.push("exe");
                                    Process {
                                        pid,
                                        description: std::fs::read_link(path)
                                            .ok()
                                            .and_then(|path| {
                                                path.file_name()
                                                    .map(|name| name.to_string_lossy().to_string())
                                            })
                                            .unwrap_or_default(),
                                    }
                                })
                            })
                            .filter(
                                |&Process {
                                     pid,
                                     description: _,
                                 }| {
                                    i32::try_from(pid)
                                        .ok()
                                        .is_some_and(|pid| kill(Pid::from_raw(pid), None).is_ok())
                                },
                            )
                    })
                    .collect::<Vec<Process>>()
            })
            .unwrap_or_default())
    }

    struct AutoDropFile(PathBuf);

    impl AutoDropFile {
        pub fn create(path: PathBuf) -> std::io::Result<Self> {
            File::create(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for AutoDropFile {
        fn drop(&mut self) {
            if self.0.exists() {
                std::fs::remove_file(&self.0).unwrap();
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod no_teleop {
    use quirky_binder_support::status::DynChainStatus;

    use crate::Process;

    pub fn run_chain<S, J, E>(
        _chain_status: S,
        join_chain: J,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        S: DynChainStatus + 'static,
        J: FnOnce() -> Result<(), E> + Send + 'static,
        E: Into<Box<dyn std::error::Error>> + Send + 'static,
        Box<dyn std::error::Error>: From<E>,
    {
        Ok(join_chain()?)
    }

    pub fn discover_processes() -> Result<Vec<Process>, Box<dyn std::error::Error>> {
        Ok(Vec::new())
    }
}
