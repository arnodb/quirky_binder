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
    };

    use futures::{task::LocalSpawnExt, AsyncReadExt, StreamExt};
    use quirky_binder_support::status::DynChainStatus;
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
            let cancellation_token = CancellationToken::new();

            let join_main = std::thread::spawn({
                let cancellation_token = cancellation_token.clone();
                move || {
                    let res = join_chain();
                    cancellation_token.cancel();
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

            let mut conn_stream = pin!(listen(cancellation_token.clone()));
            while let Some(stream) = conn_stream.next().await {
                let (stream, _addr) = stream?;
                if let Err(e) = spawn.spawn_local({
                    let cancellation_token = cancellation_token.clone();
                    let client = client.client.hook.clone();
                    async move {
                        let (input, output) = stream.split();
                        run_server_connection(input, output, client, cancellation_token).await;
                    }
                }) {
                    eprintln!("Error while spawning connection handler: {e}");
                }
            }

            join_main
                .join()
                .map_err(|_err| "Unable to join main thread".to_owned())??;

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
