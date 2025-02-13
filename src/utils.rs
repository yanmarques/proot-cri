// Copyright 2025 yanmarques, BrunoMeyer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    fs::{self, FileType},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use nix::{
    sys::wait::{WaitPidFlag, WaitStatus},
    unistd::Pid,
};
use oci_client::client::ImageData;

/// Parse "WWW-Authenticate" header format
pub fn parse_www_authenticate(header: &str) -> Option<HashMap<String, String>> {
    let mut parts = header.splitn(2, " ");
    let _ = parts.next()?.to_string();
    let params = parts.next().unwrap_or("");

    let mut param_map = HashMap::new();
    for param in params.split(",") {
        let mut kv = param.trim().splitn(2, "=");
        if let (Some(key), Some(value)) = (kv.next(), kv.next()) {
            let value = value.trim_matches('"').to_string();
            param_map.insert(key.to_string(), value);
        }
    }

    Some(param_map)
}

/// Get current timestamp of source time
pub fn to_timestamp(source: SystemTime) -> Result<i64, anyhow::Error> {
    let now = source.duration_since(UNIX_EPOCH)?;

    let ts = i64::try_from(now.as_nanos())?;
    Ok(ts)
}

/// Get current timestamp
pub fn timestamp() -> Result<i64, anyhow::Error> {
    let now = SystemTime::now();
    to_timestamp(now)
}

/// Recursively copy all files from `src` to `dest`.
pub fn copy_dir_all<S, D>(src: S, dest: D) -> Result<(), anyhow::Error>
where
    S: AsRef<Path>,
    D: AsRef<Path>,
{
    fs::create_dir_all(&dest)?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;

        let file_type = entry.file_type()?;

        // TODO: copy symbolic links
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &dest.as_ref().join(entry.file_name()))?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), dest.as_ref().join(entry.file_name()))?;
        }
    }

    Ok(())
}

/// Attempts to collect the exitcode of the process id to exit and return immediately otherwise
pub fn try_wait_pid(pid: i32) -> Option<i32> {
    if let Ok(status) = nix::sys::wait::waitpid(
        Pid::from_raw(pid.try_into().expect("bug? pid is too large")),
        Some(WaitPidFlag::WNOHANG),
    ) {
        match status {
            WaitStatus::Exited(_, code) => {
                return Some(code);
            }
            WaitStatus::Signaled(_, _, _) => {}
            WaitStatus::Stopped(_, _) => {}
            WaitStatus::PtraceEvent(_, _, _) => {}
            WaitStatus::PtraceSyscall(_) => {}
            WaitStatus::Continued(_) => {}
            WaitStatus::StillAlive => {}
        }
    }

    None
}

/// Get the image digest from given image
pub fn digest_to_image_id(data: &ImageData) -> String {
    let digest = data
        .digest
        .as_ref()
        .expect("bug? oci-distribution did not return image digest");

    let image_id = digest
        .split_once(":")
        .and_then(|(_, id)| Some(id.to_string()))
        .unwrap_or_else(|| digest.clone());

    return image_id;
}
