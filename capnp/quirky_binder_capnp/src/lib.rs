capnp::generated_code!(pub mod quirky_binder_capnp);

pub mod state;

// Until further developments, quirky_binder declares that teleop is only supported on "linux".

#[cfg(target_os = "linux")]
pub use linux::run_chain;
#[cfg(not(target_os = "linux"))]
pub use no_teleop::run_chain;

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        pin::pin,
        sync::{Arc, LazyLock},
        time::{Duration, Instant},
    };

    use futures::{task::LocalSpawnExt, AsyncReadExt, FutureExt, StreamExt};
    use quirky_binder_support::status::DynChainStatus;
    use smol::{lock::Mutex, net::unix::UnixStream, Timer};
    use teleop::{
        attach::unix_socket::listen,
        cancellation::CancellationToken,
        operate::capnp::{run_server_connection, teleop_capnp, TeleopServer},
    };

    use crate::{quirky_binder_capnp, state::StateServer};

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
        let pid = std::process::id();
        println!("PID: {pid}");

        let mut exec = futures::executor::LocalPool::new();
        let spawn = exec.spawner();

        exec.run_until(async {
            let end_of_chain_token = CancellationToken::new();

            let join_main = std::thread::spawn({
                let end_of_chain_token = end_of_chain_token.clone();
                move || {
                    let res = join_chain();
                    end_of_chain_token.cancel();
                    res
                }
            });

            let client = LazyLock::new(|| {
                let mut server = TeleopServer::new();
                server
                    .register_service::<quirky_binder_capnp::state::Client, _, _>("state", || {
                        StateServer::new(Arc::new(chain_status))
                    });
                capnp_rpc::new_client::<teleop_capnp::teleop::Client, _>(server)
            });

            let connection_count = Arc::new(Mutex::new(0));

            let main_cancellation_token = CancellationToken::new();

            let handle_new_connection =
                async |stream: UnixStream,
                       end_of_chain_token: CancellationToken,
                       main_cancellation_token: CancellationToken| {
                    {
                        let mut count = connection_count.lock().await;
                        (*count) += 1;
                    }
                    if let Err(e) = spawn.spawn_local({
                        let connection_count = connection_count.clone();
                        let end_of_chain_token = end_of_chain_token.clone();
                        let main_cancellation_token = main_cancellation_token.clone();
                        let client = client.client.hook.clone();
                        async move {
                            let (input, output) = stream.split();
                            futures::select! {
                                res = run_server_connection(input, output, client).fuse() => {
                                    if let Err(err) = res {
                                        eprintln!("Error while running server connection: {err}");
                                    }
                                }
                                () = main_cancellation_token.cancelled().fuse() => {}
                            }
                            {
                                let mut count = connection_count.lock().await;
                                (*count) -= 1;
                                if end_of_chain_token.is_cancelled() && *count == 0 {
                                    main_cancellation_token.cancel();
                                }
                            }
                        }
                    }) {
                        eprintln!("Error while spawning connection handler: {e}");
                        {
                            let mut count = connection_count.lock().await;
                            (*count) -= 1;
                            if end_of_chain_token.is_cancelled() && *count == 0 {
                                main_cancellation_token.cancel();
                            }
                        }
                    }
                };

            let mut conn_stream = pin!(listen());

            loop {
                futures::select! {
                    stream = conn_stream.next().fuse() => {
                        if let Some(stream) = stream {
                            let (stream, _addr) = stream?;
                            handle_new_connection(
                                stream,
                                end_of_chain_token.clone(),
                                main_cancellation_token.clone(),
                            ).await;
                        } else {
                            break;
                        }
                    }
                    () = end_of_chain_token.cancelled().fuse() => {
                        break;
                    }
                }
            }

            join_main
                .join()
                .map_err(|_err| "Unable to join main thread".to_owned())??;

            let deadline = Instant::now() + Duration::from_millis(10000);

            eprintln!("Waiting for end of teleoperations...");
            loop {
                futures::select! {
                    // Still accept connections (could be a late first one)
                    stream = conn_stream.next().fuse() => {
                        if let Some(stream) = stream {
                            let (stream, _addr) = stream?;
                            handle_new_connection(
                                stream,
                                end_of_chain_token.clone(),
                                main_cancellation_token.clone(),
                            ).await;
                        } else {
                            break;
                        }
                    }
                    () = main_cancellation_token.cancelled().fuse() => {
                        eprintln!("All teleop connections closed");
                        break;
                    }
                    _ = FutureExt::fuse(Timer::at(deadline)) => {
                        eprintln!("Timeout");
                        main_cancellation_token.cancel();
                        break;
                    }
                }
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        })?;

        exec.run();

        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod no_teleop {
    use quirky_binder_support::status::DynChainStatus;

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
}
