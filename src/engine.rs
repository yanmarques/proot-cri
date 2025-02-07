use std::{
    borrow::BorrowMut,
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread::sleep,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use flate2::read::GzDecoder;
use nix::{
    sys::{
        signal::Signal,
        wait::{WaitPidFlag, WaitStatus},
    },
    unistd::Pid,
};
use tracing::debug;

use crate::cri::runtime::ContainerConfig;

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
    containers: Arc<Mutex<HashMap<String, PRootProc>>>,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            ..Default::default()
        }
    }

    /// Stop all existing containers and return all errors encountered along the way
    pub fn shutdown(self) -> Vec<(String, Result<i32, anyhow::Error>)> {
        let mut lock = self.containers.lock().expect("lock poisoned");
        lock.borrow_mut()
            .iter_mut()
            .map(|(id, container)| {
                let init_pid = container.init_pid;
                let proot_pid = container.child.id();

                // close stdin to avoid deadlock
                drop(container.child.stdin.take());

                match self.check_zombie_process(container) {
                    Ok(None) => {}
                    Ok(Some(exitcode)) => {
                        return (id.clone(), Ok(exitcode));
                    }
                    Err(error) => {
                        return (id.clone(), Err(error));
                    }
                };

                (id.clone(), self.inner_stop(init_pid, proot_pid, id, None))
            })
            .collect()
    }

    // Stop an existing container
    //
    // The way the container is stopped would not be considered trivial. Because proot uses ptrace
    // to simulate an isolated environment, sending a SIGTERM to the child entrypoint process
    // (e.g., sh, python, etc.) requires significant changes to proot source code.
    // The way implemented is simpler. We first obtain the process id of the init process, and
    // store it in the `PRootProc.init_pid` attribute. Next, we send a SIGTERM to the init process.
    // Finally, we obtain the exit code of the init process through the exit code of proot.
    pub fn stop(&self, id: &String, timeout: Option<Duration>) -> Result<i32, anyhow::Error> {
        let mut lock = self.containers.lock().expect("lock poisoned");
        let container = lock
            .borrow_mut()
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("unknown container"))?;

        let init_pid = container.init_pid;
        let proot_pid = container.child.id();

        // close stdin to avoid deadlock
        drop(container.child.stdin.take());

        match self.check_zombie_process(container) {
            Ok(None) => {}
            Ok(Some(exitcode)) => {
                drop(lock.borrow_mut().remove(id));
                return Ok(exitcode);
            }
            Err(error) => {
                drop(lock.borrow_mut().remove(id));
                return Err(error);
            }
        };

        // don't keep the lock while trying to and for the container process to wait
        drop(lock);

        self.inner_stop(init_pid, proot_pid, id, timeout)
    }

    fn inner_stop(
        &self,
        init_pid: u32,
        proot_pid: u32,
        id: &String,
        timeout: Option<Duration>,
    ) -> Result<i32, anyhow::Error> {
        // TODO: also check proot process stopped

        debug!(?init_pid, "stopping container");

        // process id of proot
        let proot_pid = Pid::from_raw(proot_pid.try_into().expect("bug? pid is too large"));

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
            // wait for pid to exit non-block - similar to the `try_wait()` implementation
            if let Ok(status) = nix::sys::wait::waitpid(proot_pid, Some(WaitPidFlag::WNOHANG)) {
                match status {
                    WaitStatus::Exited(_, code) => {
                        exitcode = code;
                        exited = true;
                        break;
                    }
                    WaitStatus::Signaled(_, _, _) => {}
                    WaitStatus::Stopped(_, _) => {}
                    WaitStatus::PtraceEvent(_, _, _) => {}
                    WaitStatus::PtraceSyscall(_) => {}
                    WaitStatus::Continued(_) => {}
                    WaitStatus::StillAlive => {}
                }
            }

            let elapsed = now.elapsed()?;
            if elapsed.as_secs() > wait_timeout {
                break;
            }

            sleep(Duration::from_millis(200));
        }

        let mut lock = self.containers.lock().expect("lock poisoned");
        drop(lock.borrow_mut().remove(id));
        drop(lock);

        if !exited {
            // the processes did not exited yet
            // we have to SIGKILL proot process, which will send a SIGKILL to the init process
            nix::sys::signal::kill(proot_pid, Signal::SIGKILL)?;
        }

        debug!(exitcode, "container stopped");

        Ok(exitcode)
    }

    fn check_zombie_process(
        &self,
        container: &mut PRootProc,
    ) -> Result<Option<i32>, anyhow::Error> {
        if container.init_pid == 0 {
            // avoid a zombie proot process
            if let Ok(Some(status)) = container.child.try_wait() {
                if let Some(exitcode) = status.code() {
                    return Ok(Some(exitcode));
                }
            }

            return Err(anyhow::anyhow!("container failed to start"));
        }

        Ok(None)
    }

    // Start a ne container
    pub fn spawn(
        &self,
        id: &String,
        config: &ContainerConfig,
        container_dir: PathBuf,
        image_layers: Vec<PathBuf>,
    ) -> Result<(u32, u32), anyhow::Error> {
        let mut lock = self.containers.lock().expect("lock poisoned");
        if lock.borrow_mut().get(id).is_some() {
            return Err(anyhow::anyhow!("container already exists"));
        }
        drop(lock);

        let mut args: Vec<String> = vec![];

        let root = container_dir.join("mounts").join("root");
        args.push("-r".to_string());
        args.push(root.to_str().expect("root directory").to_string());

        // TODO: mount a private /dev/ and /proc/
        args.push("-b".to_string());
        args.push("/dev/".to_string());
        args.push("-b".to_string());
        args.push("/proc/".to_string());

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

        let mut write_init_pid = tempfile::NamedTempFile::new()?;

        // reasonable default environment vars, all those seem to be required to many programs
        let mut default_envs = HashMap::from([
            ("PATH", "/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin"),
            ("HOME", "/"),
            ("SHELL", "/bin/sh"),
            ("TERM", "xterm"),
            ("PROOT_TMP_DIR", "/data/local/tmp"),
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

        let mut lock = self.containers.lock().expect("lock poisoned");
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
