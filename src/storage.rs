use std::{
    fmt::Write as FmtWrite,
    fs::{self, File},
    io::{Read, Write as IoWrite},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Context;
use prost::Message;
use rand::TryRngCore;
use serde::{de::Deserializer, Deserialize};

use crate::{
    cri::runtime::{
        Container, ContainerConfig, ContainerState, ContainerStatus, Image, LinuxPodSandboxStatus,
        Namespace, NamespaceOption, PodSandbox, PodSandboxConfig, PodSandboxNetworkStatus,
        PodSandboxState, PodSandboxStatus,
    },
    errors::StorageError,
    utils::{self, to_timestamp},
};

/// Generate hex string of size
pub fn random_id() -> Result<String, anyhow::Error> {
    let mut bytes = [0; 32];
    rand::rng().try_fill_bytes(&mut bytes)?;
    let mut hex_string = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut hex_string, "{:02x}", byte)?;
    }
    Ok(hex_string)
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ContainerInfo {
    #[prost(string)]
    pub pod_sandbox_id: String,
    #[prost(message)]
    pub config: Option<ContainerConfig>,
    #[prost(int64)]
    pub created_at: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PodSandboxInfo {
    #[prost(message)]
    pub config: Option<PodSandboxConfig>,
    #[prost(enumeration = "PodSandboxState")]
    pub state: i32,
    #[prost(int64)]
    pub created_at: i64,
}

#[derive(Clone, PartialEq, prost::Message, serde::Deserialize)]
pub struct BuildConfig {
    #[serde(rename = "Entrypoint")]
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    #[prost(string, repeated)]
    pub entrypoint: prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[serde(rename = "Cmd")]
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    #[prost(string, repeated)]
    pub args: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[serde(rename = "Env")]
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    #[prost(string, repeated)]
    pub env: prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[serde(rename = "WorkingDir")]
    #[serde(default)]
    #[prost(string, optional)]
    pub working_dir: Option<String>,
}

/// Custom function to convert `null` to an empty Vec<String>
fn deserialize_null_as_empty_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<Vec<String>>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default()) // Convert `None` (null) to `vec![]`
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ImageInfo {
    #[prost(message)]
    pub image: Option<Image>,
    #[prost(bytes = "vec")]
    pub raw_info: prost::alloc::vec::Vec<u8>,
    #[prost(message)]
    pub build_config: Option<BuildConfig>,
}

/// Image layer
#[derive(Debug)]
pub struct ImageLayer {
    /// Digest (e.g., "sha256:abcdef..")
    pub digest: String,

    #[allow(dead_code)]
    /// Size in bytes
    pub size: u64,
}

/// Remote container image information
#[derive(Debug)]
pub struct RemoteImage {
    pub domain: String,
    pub repository: String,
    pub tag: String,
}

impl RemoteImage {
    /// Parse remote image info from image name (e.g., public.ecr.aws/alpine:v3.19, alpine, etc.)
    pub fn parse(name: &String) -> Result<RemoteImage, anyhow::Error> {
        if name.len() == 0 {
            return Err(anyhow::anyhow!("invalid empty reference image string"));
        }

        let (domain, repo) = match name.split_once("/") {
            Some((domain, name)) => (domain.to_string(), name.to_string()),
            None => (
                "registry-1.docker.io".to_string(),
                format!("library/{}", name),
            ),
        };

        let (repository, tag) = match repo.split_once(":") {
            Some((repo, tag)) => (repo.to_string(), tag.to_string()),
            None => (repo, "latest".to_string()),
        };

        return Ok(RemoteImage {
            domain,
            repository,
            tag,
        });
    }
}

impl ToString for RemoteImage {
    /// Return text representation of remote image.
    /// Note: the returned string is not guaranteed to equal the original parsed string.
    fn to_string(&self) -> String {
        format!("{}/{}:{}", self.domain, self.repository, self.tag)
    }
}

/// Storage of the containers and images
#[derive(Debug, Clone)]
pub struct Storage {
    root: PathBuf,
    containers: PathBuf,
    images: PathBuf,
    layers: PathBuf,
    pods: PathBuf,
}

// This implementation is designed to rely on file system as the underlying storage mechanism,
// not only to store data, but define the data model.
//
// Expected structure of the file system structure:
//   images/
//    \_ library/alpine/
//    |   \_ layers/
//    |       \_ abcdef....tar.gz -> ../../../layers/abcdef...tar.gz (symbolic link)
//   layers
//    \_ abcdef....tar.gz
//
impl Storage {
    pub fn new(root: &str) -> Result<Storage, anyhow::Error> {
        let root_path = std::path::absolute(Path::new(root))?;
        let containers = root_path.join("containers");
        let images = root_path.join("images");
        let layers = root_path.join("layers");
        let pods = root_path.join("pods");
        Ok(Storage {
            root: root_path.to_path_buf(),
            containers,
            images,
            layers,
            pods,
        })
    }

    /// initialize storage
    pub fn init(&self) -> Result<(), anyhow::Error> {
        if !self.root.exists() {
            fs::create_dir_all(&self.root)?;
        }

        if !self.containers.exists() {
            fs::create_dir_all(&self.containers)?;
        }

        if !self.images.exists() {
            fs::create_dir_all(&self.images)?;
        }

        if !self.layers.exists() {
            fs::create_dir_all(&self.layers)?;
        }

        if !self.pods.exists() {
            fs::create_dir_all(&self.pods)?;
        }

        Ok(())
    }

    pub fn mountpoint(&self) -> PathBuf {
        std::path::absolute(&self.root).expect("mountpoint")
    }

    pub fn list_containers(&self) -> Result<Vec<Container>, anyhow::Error> {
        let mut containers = vec![];

        for entry in self.containers.read_dir()? {
            if let Ok(entry) = entry {
                let path = entry.path();

                if path.is_dir() {
                    let id = path
                        .file_name()
                        .expect("bug? unknown container path")
                        .to_str()
                        .expect("bug? container name has invalid chars")
                        .to_string();

                    let data = self.get_container_config(&id)?;

                    let (state, created) =
                        self.lookup_container_state_and_created_at(&path, &data)?;

                    let config = data.config.expect("bug? container data has missing config");

                    let image = config.image.clone();

                    let container = Container {
                        id,
                        pod_sandbox_id: data.pod_sandbox_id,
                        image: image.clone(),
                        state: state.into(),
                        created_at: created,
                        image_ref: image
                            .and_then(|i| Some(i.image))
                            .unwrap_or_else(|| String::new()),
                        metadata: config.metadata,
                        labels: config.labels,
                        annotations: config.annotations,
                    };

                    containers.push(container);
                }
            }
        }

        Ok(containers)
    }

    fn lookup_container_state_and_created_at(
        &self,
        path_to_container: &PathBuf,
        info: &ContainerInfo,
    ) -> Result<(ContainerState, i64), anyhow::Error> {
        let mut state = ContainerState::ContainerUnknown;

        if path_to_container.join("exitcode").exists() {
            state = ContainerState::ContainerExited;
        }

        if path_to_container.join("pid").exists() {
            state = ContainerState::ContainerRunning;
        }

        // TODO: rust android does not support creation time at the time
        let created = info.created_at;

        Ok((state, created))
    }

    /// add new container
    pub fn add_container(
        &self,
        container: &ContainerConfig,
        pod_sandbox_id: &String,
    ) -> Result<String, anyhow::Error> {
        let id = random_id()?;
        let root = self.containers.join(&id);

        if !root.exists() {
            fs::create_dir_all(&root)?;
        }

        let container = ContainerInfo {
            config: Some(container.clone()),
            pod_sandbox_id: pod_sandbox_id.clone(),
            created_at: utils::timestamp()?,
        };

        let mut buf = vec![];
        container.encode(&mut buf)?;

        let mut file = File::create(root.join("spec.bin"))?;
        file.write_all(&buf)?;

        fs::create_dir_all(self.build_container_path(&id).join("mounts"))?;

        Ok(id)
    }

    /// Determines whether given container exists by its ID.
    /// Returns a file system path to the container, if it exists.
    pub fn has_container(&self, id: &String) -> Option<PathBuf> {
        let root = self.build_container_path(id);
        if root.exists() {
            Some(root)
        } else {
            None
        }
    }

    /// Get status information of given container. Returns `Ok(None)` if container does not exist.
    pub fn get_container_status(
        &self,
        id: &String,
    ) -> Result<(String, Option<ContainerStatus>), anyhow::Error> {
        let root = self.build_container_path(id);

        if !root.exists() {
            return Err(anyhow::anyhow!(StorageError::ContainerNotFound(id.clone())));
        }

        let data = self.get_container_config(id)?;
        let (state, created) = self.lookup_container_state_and_created_at(&root, &data)?;

        let config = data.config.expect("bug? container data has missing config");

        let exitcode_path = root.join("exitcode");
        let mut exit_code = 0;

        if exitcode_path.exists() {
            let mut buf = [0; 4];
            let mut file = File::open(exitcode_path)?;
            file.read(&mut buf)?;
            exit_code = i32::from_le_bytes(buf);
        }

        let log_path = root.join("out.log").to_str().expect("log path").to_string();

        let status = ContainerStatus {
            id: id.clone(),
            log_path,
            created_at: created,
            exit_code,
            finished_at: 0,
            started_at: 0,
            message: String::new(),
            state: state.into(),
            image: config.image.clone(),
            annotations: config.annotations,
            image_ref: config
                .image
                .and_then(|i| Some(i.image))
                .unwrap_or_else(|| String::new()),
            labels: config.labels,
            metadata: config.metadata,
            mounts: config.mounts,
            reason: String::new(),
        };

        Ok((data.pod_sandbox_id, Some(status)))
    }

    /// Get underlying container config. Returns error if given container does not exists.
    pub fn get_container_config(&self, id: &String) -> Result<ContainerInfo, anyhow::Error> {
        let path = self.build_container_path(id);

        // TODO: optimize memory allocations
        let mut buf: Vec<u8> = vec![];

        let mut file =
            File::open(path.join("spec.bin")).context(format!("opening spec at {:?}", &path))?;
        file.read_to_end(&mut buf)?;

        let data: ContainerInfo =
            prost::Message::decode(&buf[..]).context(format!("decode container {:?}", &path))?;

        Ok(data)
    }

    /// Delete all files associated with given container ID if it exists. If the container does not
    /// exist, returns `Ok(None)`
    pub fn remove_container(&self, id: &String) -> Result<(), anyhow::Error> {
        if let Some(root) = self.has_container(id) {
            fs::remove_dir_all(root)?;
            Ok(())
        } else {
            anyhow::bail!(StorageError::ContainerNotFound(id.clone()))
        }
    }

    /// Truncate the log file of the given container, if it exists.
    pub fn reopen_container_log(&self, id: &String) -> Result<(), anyhow::Error> {
        if let Some(root) = self.has_container(id) {
            fs::write(root.join("out.log"), &[])?;
            Ok(())
        } else {
            anyhow::bail!(StorageError::ContainerNotFound(id.clone()))
        }
    }

    /// save exit code of given container and removes
    /// any existing saved process id
    pub fn save_container_exitcode(&self, id: &String, exitcode: i32) -> Result<(), anyhow::Error> {
        let mut file = File::create(self.build_container_path(id).join("exitcode"))?;
        file.write_all(&exitcode.to_le_bytes())?;

        fs::remove_file(self.build_container_path(id).join("pid"))
            .expect("container's pid file should exist");

        Ok(())
    }

    /// save process id of the given container
    pub fn save_container_pid(&self, id: &String, pid: u32) -> Result<(), anyhow::Error> {
        let mut file = File::create(self.build_container_path(id).join("pid"))?;
        file.write_all(pid.to_string().as_bytes())?;
        Ok(())
    }

    /// Find and return existing image. If image does not exist, returns `None`.
    pub fn get_image(&self, image_id: &String) -> Result<Option<ImageInfo>, anyhow::Error> {
        let path = self.build_path_to_image(image_id);

        if !path.exists() {
            return Ok(None);
        }

        let buf = fs::read(path.join("image.bin"))?;
        let image: ImageInfo = prost::Message::decode(&buf[..])?;

        Ok(Some(image))
    }

    /// Return whether image layer exists by the layer digest
    pub fn has_image_layer(&self, layer: &String) -> bool {
        let layer_dir = self.build_image_layer_path(layer);

        layer_dir.exists() && layer_dir.join("layer.tar.gz").exists()
    }

    // add image layer, overwriting existing if needed
    pub fn add_image_layer(
        &self,
        layer: &oci_client::client::ImageLayer,
    ) -> Result<(), anyhow::Error> {
        let layer_path = self.build_image_layer_path(&layer.sha256_digest());
        let parent = layer_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("bug: it should have a parent here"))?;

        if !parent.exists() {
            fs::create_dir_all(&parent)?;
        }

        let mut file = File::create(layer_path)?;
        file.write_all(&layer.data)?;
        Ok(())
    }

    /// Add image and associate with given existing layers.
    /// Returns `Ok(true)` if image already exists, and do not overwrite it.
    pub fn add_image(
        &self,
        image: oci_client::Reference,
        data: oci_client::client::ImageData,
    ) -> Result<String, anyhow::Error> {
        let image_id = utils::digest_to_image_id(&data);

        let root = self.build_path_to_image(&image_id);
        let image_layers_path = self.build_path_to_image_layers(&image_id);

        if root.exists() {
            anyhow::bail!("image already exits");
        }

        fs::create_dir_all(&image_layers_path)?;

        let mut config = serde_json::from_slice::<serde_json::Value>(&data.config.data)?;
        let raw = config["config"].take();
        tracing::info!(?raw, "config raw");
        let build_config: BuildConfig = serde_json::from_value(raw)?;

        let spec = ImageInfo {
            image: Some(Image {
                id: image_id.clone(),
                repo_tags: vec![image.to_string()],
                size: 1,
                ..Default::default()
            }),
            raw_info: data.config.data,
            build_config: Some(build_config),
        };

        let mut buf = vec![];
        spec.encode(&mut buf)?;

        let mut file = File::create(root.join("image.bin"))?;
        file.write_all(&buf)?;
        drop(file);

        for (idx, layer) in data.layers.iter().enumerate() {
            let mut file = File::create(image_layers_path.join(idx.to_string()))?;
            file.write_all(layer.sha256_digest().as_bytes())?;
        }

        Ok(image_id)
    }

    /// Return list of all stored images.
    pub fn list_images(&self) -> Result<Vec<Image>, anyhow::Error> {
        let mut images = vec![];

        for entry in self.images.read_dir()? {
            if let Ok(entry) = entry {
                let path = entry.path();

                if path.is_dir() {
                    let buf = fs::read(path.join("image.bin"))?;
                    let info: ImageInfo = prost::Message::decode(&buf[..])?;

                    images.push(info.image.expect("bug? stored incomplete image"));
                }
            }
        }

        Ok(images)
    }

    // remove given image from storage if possible
    pub fn remove_image(&self, _image: &RemoteImage) -> Result<(), anyhow::Error> {
        // TODO: actually remove only the provided image, not all of them
        fs::remove_dir_all(&self.images)?;
        fs::create_dir_all(&self.images)?;

        Ok(())
    }

    // get path to all images layers used by given image
    pub fn image_layers(&self, image_id: &String) -> Result<Vec<PathBuf>, anyhow::Error> {
        let root = self.build_path_to_image_layers(image_id);
        if !root.exists() {
            return Err(anyhow::anyhow!("unknown image: {image_id:?}"));
        }

        let mut entries = vec![];
        for entry in root.read_dir()? {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    let mut buf = vec![];
                    let mut file = File::open(&path)?;
                    file.read_to_end(&mut buf)?;

                    let digest = String::from_utf8(buf)?;
                    let layer = self.build_image_layer_path(&digest);

                    let idx: u64 = path
                        .file_name()
                        .expect("bug? layer should have file name")
                        .to_str()
                        .expect("bug? in fs")
                        .parse()?;

                    entries.push((idx, layer));
                }
            }
        }

        entries.sort_by(|(idx_a, _), (idx_b, _)| idx_a.cmp(idx_b));

        Ok(entries.into_iter().map(|(_, path)| path).collect())
    }

    /// list pod sandbox
    pub fn list_pod_sandboxes(&self) -> Result<Vec<PodSandbox>, anyhow::Error> {
        let mut pods = vec![];

        for entry in self.pods.read_dir()? {
            if let Ok(entry) = entry {
                let path = entry.path();

                if path.is_dir() {
                    let id = path
                        .file_name()
                        .expect("bug? unknown container path")
                        .to_str()
                        .expect("bug? container name has invalid chars")
                        .to_string();

                    // TODO: optimize memory allocations
                    let mut buf: Vec<u8> = vec![];

                    let mut file = File::open(path.join("spec.bin"))
                        .context(format!("opening spec at {:?}", &path))?;
                    file.read_to_end(&mut buf)?;

                    let info: PodSandboxInfo = prost::Message::decode(&buf[..])
                        .context(format!("decode podsandbox {:?}", &path))?;

                    let config = info.config.expect("bug? pod sandbox info missing config");

                    let pod = PodSandbox {
                        id,
                        state: info.state.into(),
                        labels: config.labels,
                        metadata: config.metadata,
                        annotations: config.annotations,
                        created_at: info.created_at.into(),
                        runtime_handler: "".to_string(),
                    };

                    pods.push(pod);
                }
            }
        }

        Ok(pods)
    }

    pub fn get_pod_sandbox_status(&self, id: &String) -> Result<PodSandboxStatus, anyhow::Error> {
        let path = self.pods.join(id);

        // TODO: optimize memory allocations
        let mut buf: Vec<u8> = vec![];

        let mut file =
            File::open(path.join("spec.bin")).context(format!("opening spec at {:?}", &path))?;
        file.read_to_end(&mut buf)?;

        let info: PodSandboxInfo =
            prost::Message::decode(&buf[..]).context(format!("decode podsandbox {:?}", &path))?;

        let ns_options = info
            .config
            .clone()
            .and_then(|c| c.linux)
            .and_then(|l| l.security_context)
            .and_then(|s| s.namespace_options);

        let config = info.config.expect("bug? pod sandbox info missing config");

        let status = PodSandboxStatus {
            id: id.clone(),
            annotations: config.annotations,
            labels: config.labels,
            metadata: config.metadata,
            created_at: info.created_at,
            state: info.state.into(),
            linux: Some(LinuxPodSandboxStatus {
                namespaces: Some(Namespace {
                    options: ns_options,
                }),
            }),
            network: Some(PodSandboxNetworkStatus::default()),
            ..Default::default()
        };

        Ok(status)
    }

    /// add pod sandbox
    pub fn add_pod_sandbox(&self, pod: PodSandboxConfig) -> Result<String, anyhow::Error> {
        let id = random_id()?;
        let root = self.pods.join(&id);

        if !root.exists() {
            fs::create_dir_all(&root)?;
        }

        let info = PodSandboxInfo {
            config: Some(pod),
            state: PodSandboxState::SandboxReady.into(),
            created_at: utils::timestamp()?,
        };

        let mut buf = vec![];
        info.encode(&mut buf)?;

        let mut file = File::create(root.join("spec.bin"))?;
        file.write_all(&buf)?;

        Ok(id)
    }

    /// Change the state of the pod sandbox to not ready
    pub fn stop_pod_sandbox(&self, pod_id: &String) -> Result<(), anyhow::Error> {
        let path = self.pods.join(&pod_id);

        if !path.exists() {
            anyhow::bail!("unknown pod sandbox")
        }

        // TODO: optimize memory allocations
        let mut buf: Vec<u8> = vec![];

        let mut file =
            File::open(path.join("spec.bin")).context(format!("opening spec at {:?}", &path))?;
        file.read_to_end(&mut buf)?;

        let mut info: PodSandboxInfo =
            prost::Message::decode(&buf[..]).context(format!("decode podsandbox {:?}", &path))?;

        info.state = PodSandboxState::SandboxNotready.into();

        let mut buf = vec![];
        info.encode(&mut buf)?;

        let mut file = File::create(path.join("spec.bin"))?;
        file.write_all(&buf)?;

        Ok(())
    }

    /// Remove the pod sandbox from the storage.
    /// TODO: sanity checks: can I remove all containers? etc.
    pub fn remove_pod_sandbox(&self, pod: &String) -> Result<(), anyhow::Error> {
        // TODO: remove dependent containers
        let root = self.pods.join(&pod);

        Ok(fs::remove_dir_all(&root)?)
    }

    /// Build the file system path to the given image digest.
    /// It assumes the image exists without performing any confirmation, it just builds the path.
    pub fn build_path_to_image(&self, digest: &String) -> PathBuf {
        self.images.join(&digest)
    }

    /// Build the file system path to the private layers of the given image digest.
    /// It assumes the image exists without performing any confirmation, it just builds the path.
    pub fn build_path_to_image_layers(&self, digest: &String) -> PathBuf {
        self.build_path_to_image(digest).join("layers")
    }

    /// Build the file system path to the container by the given ID.
    /// It assumes the container exists without performing any confirmation, it just builds the path.
    pub fn build_container_path(&self, id: &String) -> PathBuf {
        self.containers.join(id)
    }

    /// Build path to the given image layer. An image layer is a global file,
    /// where all dependent images are associated.
    pub fn build_image_layer_path(&self, layer: &String) -> PathBuf {
        self.layers.join(layer).join("layer.tar.gz")
    }
}
