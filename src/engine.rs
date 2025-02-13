// android-cri
// Copyright (C) 2025 yanmarques
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use std::{
    borrow::BorrowMut,
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use flate2::read::GzDecoder;
use nix::{sys::signal::Signal, unistd::Pid};
use tokio::sync::RwLock;
use tracing::debug;

use crate::{cri::runtime::ContainerConfig, utils};

/// Holds the proot process that run the container
#[derive(Debug)]
pub struct PRootProc {
    /// The underlying system process
    child: Child,

    /// The process id of the entrypoint
    init_pid: u32,
}

/// Container engine
#[derive(Debug, Default, Clone)]
pub struct Engine {
    containers: Arc<RwLock<HashMap<String, PRootProc>>>,
}

impl Engine {
    /// Main method to create a new engine.
    pub fn new() -> Engine {
        Engine {
            ..Default::default()
        }
    }

    /// Attempts to collect the exit code of all running containers. It does not hang waiting for
    /// any container to exit, only containers that already exited will be collected. All
    /// containers that exited are removed from the internal list of running containers.
    pub async fn try_wait_all_containers(&self) -> Vec<(String, i32)> {
        let mut lock = self.containers.read().await;
        let ids = lock
            .borrow_mut()
            .iter()
            .map(|(id, container)| (id.clone(), container.child.id()))
            .collect::<Vec<(String, u32)>>();

        // don't keep the lock, especially during the following multiple syscalls
        drop(lock);

        let ids: Vec<(String, i32)> = ids
            .into_iter()
            .map(|(id, pid)| {
                if let Some(exitcode) =
                    utils::try_wait_pid(pid.try_into().expect("bug? pid is too large"))
                {
                    Some((id, exitcode))
                } else {
                    None
                }
            })
            .filter(|a| a.is_some())
            .map(|a| a.unwrap())
            .collect();

        // remove containers that exited
        let mut lock = self.containers.write().await;
        for (id, _) in ids.iter() {
            drop(lock.borrow_mut().remove(id));
        }

        ids
    }

    /// Attempts to collect the exit code of the container. If we are able to collect the container
    /// exit code, it is removed from the internal list of running containers.
    pub async fn try_wait(&self, id: &String) -> Option<i32> {
        let mut lock = self.containers.read().await;
        let container = lock.borrow_mut().get(id)?;

        let pid = container.child.id();

        // don't keep the lock, especially during the following multiple syscalls
        drop(lock);

        if let Some(exitcode) = utils::try_wait_pid(pid.try_into().expect("bug? pid is too large"))
        {
            // remove containers that exited
            let mut lock = self.containers.write().await;
            drop(lock.borrow_mut().remove(id));

            Some(exitcode)
        } else {
            None
        }
    }

    /// Stop all existing containers and return all errors encountered along the way
    pub async fn shutdown(self) -> Vec<(String, Result<i32, anyhow::Error>)> {
        let mut exitcodes = vec![];

        let mut lock = self.containers.write().await;
        for (id, container) in lock.borrow_mut().iter_mut() {
            let init_pid = container.init_pid;
            let proot_pid = container.child.id();

            // close stdin to avoid deadlock
            drop(container.child.stdin.take());

            let (id, exitcode) = {
                match self.check_zombie_process(container.init_pid, container.child.id()) {
                    None => (id.clone(), self.inner_stop(init_pid, proot_pid, None).await),
                    Some(exitcode) => (id.clone(), Ok(exitcode)),
                }
            };

            exitcodes.push((id, exitcode));
        }

        exitcodes
    }

    // Stop an existing container
    //
    // The way the container is stopped would not be considered trivial. Because proot uses ptrace
    // to simulate an isolated environment, sending a SIGTERM to the child entrypoint process
    // (e.g., sh, python, etc.) requires significant changes to proot source code.
    // The way implemented is simpler. We first obtain the process id of the init process, and
    // store it in the `PRootProc.init_pid` attribute. Next, we send a SIGTERM to the init process.
    // Finally, we obtain the exit code of the init process through the exit code of proot.
    pub async fn stop(&self, id: &String, timeout: Option<Duration>) -> Result<i32, anyhow::Error> {
        let mut lock = self.containers.write().await;
        let container = lock
            .borrow_mut()
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("unknown container: {id:?}"))?;

        let init_pid = container.init_pid;
        let proot_pid = container.child.id();

        // close stdin to avoid deadlock
        drop(container.child.stdin.take());

        // don't keep the lock while trying to and for the container process to wait
        drop(lock);

        let result = self.inner_stop(init_pid, proot_pid, timeout).await;

        let mut lock = self.containers.write().await;
        drop(lock.borrow_mut().remove(id));
        drop(lock);

        result
    }

    async fn inner_stop(
        &self,
        init_pid: u32,
        proot_pid: u32,
        timeout: Option<Duration>,
    ) -> Result<i32, anyhow::Error> {
        // TODO: also check proot process stopped
        if let Some(code) = self.check_zombie_process(init_pid, proot_pid) {
            return Ok(code);
        }

        debug!(?init_pid, "stopping container");

        // process id of proot
        let proot_pid: i32 = proot_pid.try_into().expect("bug? pid is too large");

        //
        // attempt to stop container process gracefully
        //
        nix::sys::signal::kill(
            Pid::from_raw(init_pid.try_into().expect("bug? pid is too large")),
            Signal::SIGTERM,
        )?;

        // containerd default timeout is 10
        // @see https://github.com/containerd/containerd/blob/59c8cf6ea5f4175ad512914dd5ce554942bf144f/integration/nri_test.go#L48
        let wait_timeout = timeout
            .or_else(|| Some(Duration::from_secs(10)))
            .expect("stop timeout")
            .as_secs();

        let now = SystemTime::now();
        let mut exitcode: i32 = -1;
        let mut exited = false;

        loop {
            if let Some(code) = utils::try_wait_pid(proot_pid) {
                exitcode = code;
                exited = true;
                break;
            }

            let elapsed = now.elapsed()?;
            if elapsed.as_secs() > wait_timeout {
                break;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if !exited {
            // the processes did not exited yet
            // we have to SIGKILL proot process, which will send a SIGKILL to the init process
            nix::sys::signal::kill(Pid::from_raw(proot_pid), Signal::SIGKILL)?;
        }

        debug!(exitcode, "container stopped");

        Ok(exitcode)
    }

    fn check_zombie_process(&self, init_pid: u32, proot_pid: u32) -> Option<i32> {
        if init_pid == 0 {
            utils::try_wait_pid(proot_pid.try_into().expect("bug? proot pid is too large"))
        } else {
            None
        }
    }

    /// Start a new container
    pub async fn spawn(
        &self,
        id: &String,
        config: &ContainerConfig,
        container_dir: PathBuf,
        image_layers: Vec<PathBuf>,
    ) -> Result<(u32, u32), anyhow::Error> {
        let mut lock = self.containers.read().await;
        if lock.borrow_mut().get(id).is_some() {
            return Err(anyhow::anyhow!("container already exists"));
        }
        drop(lock);

        let mut args: Vec<String> = vec![];

        let root = container_dir.join("mounts").join("root");

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

        args.push("-r".to_string());
        args.push(root.to_str().expect("root directory").to_string());

        // TODO: mount a private /dev/ and /proc/
        args.push("-b".to_string());
        args.push("/dev/".to_string());
        args.push("-b".to_string());
        args.push("/proc/".to_string());

        for mount in config.mounts.iter() {
            let host_path = Path::new(&mount.host_path);
            let container_path = Path::new(&mount.container_path);

            // file system path to the container real file
            let full_path = root.join(container_path.strip_prefix("/")?);

            // TODO: support readonly by copying the existing files directory to the container
            if host_path.exists() {
                if host_path.is_dir() {
                    utils::copy_dir_all(&host_path, &full_path)?;
                } else {
                    let parent = full_path
                        .parent()
                        .expect("bug? container path does not have parent");

                    fs::create_dir_all(parent).context("failed creating parent directory")?;

                    fs::copy(&host_path, &full_path)?;
                }
            } else {
                fs::create_dir_all(&full_path)?;
            }

            // if !host_path.exists() {
            //     fs::create_dir_all(&host_path)
            //         .context("failed creating missing host path: {}")
            //         .context(format!("{:?}", host_path))?;
            // }
            //
            // // ensure destination don't already exist for the symbolic link to work correctly.
            // if full_path.exists() {
            //     if full_path.is_dir() {
            //         debug!(?full_path, "full container dir");
            //         fs::remove_dir_all(&full_path)
            //             .context("failed removing existing container dir")?;
            //     } else {
            //         fs::remove_file(&full_path)
            //             .context("failed removing existing container file")?;
            //     }
            // }

            // std::os::unix::fs::symlink(host_path, full_path).context("failed symlinking mount")?;
        }

        //
        args.push("--kill-on-exit".to_string());

        args.push("-w".to_string());
        if config.working_dir == "" {
            args.push("/".to_string());
        } else {
            args.push(config.working_dir.clone());
        }

        args.extend_from_slice(&config.command);
        args.extend_from_slice(&config.args);

        let mut write_init_pid = tempfile::NamedTempFile::new()?;

        // reasonable default environment vars, all those seem to be required to many programs
        let mut default_envs = HashMap::from([
            ("PATH", "/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin"),
            ("HOME", "/"),
            ("SHELL", "/bin/sh"),
            ("TERM", "xterm"),
            #[cfg(target_os = "android")]
            ("PROOT_TMP_DIR", "/data/local/tmp"),
            #[cfg(target_os = "linux")]
            ("PROOT_TMP_DIR", "/tmp"),
            (
                "PROOT_WRITE_INIT_PID",
                write_init_pid.path().to_str().expect("tempfile failed"),
            ),
        ]);

        for k in config.envs.iter() {
            default_envs.insert(&k.key, &k.value);
        }

        let log = File::create(container_dir.join("out.log"))?;

        debug!(?args, "cmd args");

        // TODO: do not inherit stdin, when needed, allocate a new pty for the container
        let container = Command::new("./proot")
            .args(args)
            .env_clear()
            .envs(default_envs)
            .stdin(Stdio::inherit())
            // redirect stdout and stderr to the same file descriptor
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;

        debug!(pid = container.id(), "spawned");

        //
        // proot does not handle gracefully shutting down child processes because of ptrace (I guess)
        // therefore we must get the process id of the init process to later gracefully shut it down
        //
        let timeout = 2;
        let begin = SystemTime::now();
        let mut init_pid = 0;

        loop {
            let mut buf = vec![];
            write_init_pid.read_to_end(&mut buf)?;
            if buf.len() > 0 {
                init_pid = String::from_utf8(buf)?
                    .parse::<u32>()
                    .context("parsing init pid")?;
                break;
            }

            let elapsed = begin.elapsed()?;
            if elapsed.as_secs() > timeout {
                break;
            }
        }

        if init_pid == 0 {
            // TODO: we can probably do something better when this happens, for now, we will
            // silently ignore because proot will likely exit anyway
        }

        let proot_pid = container.id();

        let mut lock = self.containers.write().await;
        lock.borrow_mut().insert(
            id.clone(),
            PRootProc {
                child: container,
                init_pid,
            },
        );

        Ok((proot_pid, init_pid))
    }
}
