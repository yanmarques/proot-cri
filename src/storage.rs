use std::{
    fmt::{self, Write as FmtWrite},
    fs::{self, File},
    io::{Read, Write as IoWrite},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Context;
use prost::{bytes::Buf, Message};
use rand::TryRngCore;

use crate::{
    cri::runtime::{
        Container, ContainerConfig, ContainerState, ContainerStatus, Image, ImageSpec, PodSandbox,
        PodSandboxConfig, PodSandboxNetworkStatus, PodSandboxState, PodSandboxStatus,
    },
    errors::StorageError,
    utils::to_timestamp,
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

                    let config = self.get_container_config(&id)?;

                    let (state, created) = self.lookup_container_state_and_created_at(&path)?;

                    let image = config.image.clone();

                    let container = Container {
                        id,
                        image: image.clone(),
                        state: state.into(),
                        created_at: created,
                        image_ref: image
                            .and_then(|i| Some(i.image))
                            .unwrap_or_else(|| String::new()),
                        metadata: config.metadata,
                        labels: config.labels,
                        annotations: config.annotations,
                        pod_sandbox_id: String::new(),
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
    ) -> Result<(ContainerState, i64), anyhow::Error> {
        let mut state = ContainerState::ContainerUnknown;

        if path_to_container.join("exitcode").exists() {
            state = ContainerState::ContainerExited;
        }

        if path_to_container.join("pid").exists() {
            state = ContainerState::ContainerRunning;
        }

        // TODO: rust android does not support creation time at the time
        let created = path_to_container
            .metadata()?
            .created()
            .unwrap_or_else(|_| SystemTime::now());

        Ok((state, to_timestamp(created)?))
    }

    /// add new container
    pub fn add_container(&self, container: &ContainerConfig) -> Result<String, anyhow::Error> {
        let id = random_id()?;
        let root = self.containers.join(&id);

        if !root.exists() {
            fs::create_dir_all(&root)?;
        }

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
    ) -> Result<Option<ContainerStatus>, anyhow::Error> {
        let root = self.build_container_path(id);

        if !root.exists() {
            return Err(anyhow::anyhow!(StorageError::ContainerNotFound(id.clone())));
        }

        let config = self.get_container_config(id)?;

        let (state, created) = self.lookup_container_state_and_created_at(&root)?;

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

        Ok(Some(status))
    }

    /// Get underlying container config. Returns error if given container does not exists.
    pub fn get_container_config(&self, id: &String) -> Result<ContainerConfig, anyhow::Error> {
        let path = self.containers.join(id);

        // TODO: optimize memory allocations
        let mut buf: Vec<u8> = vec![];

        let mut file =
            File::open(path.join("spec.bin")).context(format!("opening spec at {:?}", &path))?;
        file.read_to_end(&mut buf)?;

        let config: ContainerConfig =
            prost::Message::decode(&buf[..]).context(format!("decode container {:?}", &path))?;

        Ok(config)
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
    pub fn get_image(&self, digest: &String) -> Result<Option<Image>, anyhow::Error> {
        let path = self.build_path_to_image(digest);

        if !path.exists() {
            return Ok(None);
        }

        let buf = fs::read(path.join("image.bin"))?;
        let image: Image = prost::Message::decode(&buf[..])?;

        Ok(Some(image))
    }

    /// check whether the layer exists
    pub fn has_image_layer(&self, layer: &ImageLayer) -> bool {
        let layer_dir = self.build_image_layer_path(layer);

        layer_dir.exists()
    }

    // add image layer, overwriting existing if needed
    pub fn add_image_layer(
        &self,
        layer: &ImageLayer,
        buffer: impl Buf,
    ) -> Result<(), anyhow::Error> {
        let layer_path = self.build_image_layer_path(layer);
        let parent = layer_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("bug: it should have a parent here"))?;

        if !parent.exists() {
            fs::create_dir_all(&parent)?;
        }

        let mut file = File::create(layer_path)?;
        file.write_all(buffer.chunk())?;
        Ok(())
    }

    /// Add image and associate with given existing layers.
    /// Returns `Ok(true)` if image already exists, and do not overwrite it.
    pub fn add_image(
        &self,
        digest: &String,
        image: &RemoteImage,
        layers: &[ImageLayer],
    ) -> Result<bool, anyhow::Error> {
        let root = self.build_path_to_image(&digest);
        let image_layers_path = self.build_path_to_image_layers(&digest);

        if root.exists() {
            return Ok(true);
        }

        fs::create_dir_all(&image_layers_path)?;

        let spec = Image {
            id: digest.clone(),
            repo_tags: vec![image.to_string()],
            size: 1,
            ..Default::default()
        };

        let mut buf = vec![];
        spec.encode(&mut buf)?;

        let mut file = File::create(root.join("image.bin"))?;
        file.write_all(&buf)?;
        drop(file);

        for (idx, layer) in layers.iter().enumerate() {
            let mut file = File::create(image_layers_path.join(idx.to_string()))?;
            file.write_all(layer.digest.as_bytes())?;
        }

        Ok(false)
    }

    /// Return list of all stored images.
    pub fn list_images(&self) -> Result<Vec<Image>, anyhow::Error> {
        let mut images = vec![];

        for entry in self.images.read_dir()? {
            if let Ok(entry) = entry {
                let path = entry.path();

                if path.is_dir() {
                    let buf = fs::read(path.join("image.bin"))?;
                    let image: Image = prost::Message::decode(&buf[..])?;

                    images.push(image);
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
    pub fn image_layers(&self, digest: &String) -> Result<Vec<PathBuf>, anyhow::Error> {
        let root = self.build_path_to_image_layers(digest);
        if !root.exists() {
            return Err(anyhow::anyhow!("unknown image: {digest:?}"));
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
                    let layer = self.build_image_layer_path(&ImageLayer { digest, size: 0 });

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

                    let config: PodSandboxConfig = prost::Message::decode(&buf[..])
                        .context(format!("decode podsandbox {:?}", &path))?;

                    let created = to_timestamp(path.metadata()?.created()?)?;

                    let pod = PodSandbox {
                        id,
                        state: PodSandboxState::SandboxReady.into(),
                        labels: config.labels,
                        metadata: config.metadata,
                        annotations: config.annotations,
                        created_at: created,
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

        let config: PodSandboxConfig =
            prost::Message::decode(&buf[..]).context(format!("decode podsandbox {:?}", &path))?;

        let created = to_timestamp(
            path.metadata()?
                .created()
                .unwrap_or_else(|_| SystemTime::now()),
        )?;

        let status = PodSandboxStatus {
            id: id.clone(),
            annotations: config.annotations,
            labels: config.labels,
            metadata: config.metadata,
            created_at: created,
            state: PodSandboxState::SandboxReady.into(),
            network: Some(PodSandboxNetworkStatus {
                ip: "10.137.0.91".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(status)
    }

    /// add pod sandbox
    pub fn add_pod_sandbox(&self, pod: &PodSandboxConfig) -> Result<String, anyhow::Error> {
        let id = random_id()?;
        let root = self.pods.join(&id);

        if !root.exists() {
            fs::create_dir_all(&root)?;
        }

        let mut buf = vec![];
        pod.encode(&mut buf)?;

        let mut file = File::create(root.join("spec.bin"))?;
        file.write_all(&buf)?;

        Ok(id)
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
    pub fn build_image_layer_path(&self, layer: &ImageLayer) -> PathBuf {
        self.layers.join(&layer.digest).join("layer.tar.gz")
    }
}
