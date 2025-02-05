use std::{
    borrow::BorrowMut,
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
};

use flate2::read::GzDecoder;
use tracing::debug;

use crate::cri::runtime::ContainerConfig;

/// Container engine
#[derive(Debug, Default)]
pub struct Engine {
    containers: Arc<Mutex<Vec<Child>>>,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            ..Default::default()
        }
    }

    // Start a new container
    pub fn run(
        &self,
        config: &ContainerConfig,
        container_dir: PathBuf,
        image_layers: Vec<PathBuf>,
    ) -> Result<u32, anyhow::Error> {
        let mut args: Vec<String> = vec![];

        let root = container_dir.join("mounts").join("root");
        args.push("-r".to_string());
        args.push(root.to_str().expect("root directory").to_string());

        // TODO: mount a private /dev/ and /proc/
        args.push("-b".to_string());
        args.push("/dev/".to_string());
        args.push("-b".to_string());
        args.push("/proc/".to_string());

        args.push("-w".to_string());
        if config.working_dir == "" {
            args.push("/".to_string());
        } else {
            args.push(config.working_dir.clone());
        }

        args.extend_from_slice(&config.command);
        args.extend_from_slice(&config.args);

        //
        // extract the image, if container is new
        //
        if !root.exists() {
            // TODO: use copy-on-write to use fewer storage capacity

            fs::create_dir_all(&root)?;

            // use this single buffer to reduce allocations
            let mut buf: [u8; 8192] = [0; 8192];

            for layer in image_layers {
                let mut tar_tmp = tempfile::NamedTempFile::new()?;

                let file = File::open(layer)?;
                let mut decoder = GzDecoder::new(file);
                loop {
                    let n = decoder.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }

                    if n < buf.len() {
                        tar_tmp.write(&buf[..n])?;
                    } else {
                        tar_tmp.write(&buf)?;
                    }
                }

                // ensure all buffered writes reach the filesystem
                tar_tmp.flush()?;

                // unpack tar archive
                let mut archive = tar::Archive::new(tar_tmp.reopen()?);
                archive.unpack(&root)?;
            }
        }

        // reasonable default environment vars, all those seem to be required to many programs
        let mut default_envs = HashMap::from([
            ("PATH", "/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin"),
            ("HOME", "/"),
            ("SHELL", "/bin/sh"),
            ("TERM", "xterm"),
        ]);

        for k in config.envs.iter() {
            default_envs.insert(&k.key, &k.value);
        }

        let log = File::create(container_dir.join("out.log"))?;

        // TODO: do not inherit stdin, when needed, allocate a new pty for the container
        let mut container = Command::new("./proot")
            .args(args)
            .env_clear()
            .envs(default_envs)
            .stdin(Stdio::inherit())
            // redirect stdout and stderr to the same file descriptor
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;

        debug!(pid = container.id(), "spawned");

        // opportunity checking: identify if the process promptly exists
        if let Ok(result) = container.try_wait() {
            if let Some(exitcode) = result {
                debug!(?exitcode, "process exited");
                return Err(anyhow::anyhow!("container exited"));
            }
        }

        // save process id
        let pid = container.id();
        let mut file = File::create(container_dir.join("pid"))?;
        file.write_all(pid.to_string().as_bytes())?;

        let mut lock = self.containers.lock().expect("lock poisoned");
        lock.borrow_mut().push(container);

        Ok(pid)
    }
}
