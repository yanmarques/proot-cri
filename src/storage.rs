use std::{
    fmt::Write as FmtWrite,
    fs::{self, File},
    io::{Read, Write as IoWrite},
    path::{Path, PathBuf},
};

use anyhow::Context;
use prost::{bytes::Buf, Message};
use rand::TryRngCore;
use tracing::debug;

use crate::{
    cri::runtime::{Container, ContainerConfig, ContainerState},
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
    pub fn parse(name: String) -> Result<RemoteImage, anyhow::Error> {
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

/// Storage of the containers and images
#[derive(Debug, Clone)]
pub struct Storage {
    root: PathBuf,
    containers: PathBuf,
    images: PathBuf,
    layers: PathBuf,
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
    pub fn new(root: &str) -> Storage {
        let root_path = Path::new(root);
        let containers = root_path.join("containers");
        let images = root_path.join("images");
        let layers = root_path.join("layers");
        Storage {
            root: root_path.to_path_buf(),
            containers,
            images,
            layers,
        }
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

        Ok(())
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

                    let config = self.get_container(&id)?;

                    let mut state = ContainerState::ContainerUnknown;

                    if path.join("exitcode").exists() {
                        state = ContainerState::ContainerExited;
                    }

                    if path.join("pid").exists() {
                        state = ContainerState::ContainerRunning;
                    }

                    let created = path.metadata()?.created()?;

                    let image = config.image.clone();

                    let container = Container {
                        id,
                        image: image.clone(),
                        state: state.into(),
                        created_at: to_timestamp(created)?,
                        image_ref: image
                            .and_then(|i| Some(i.image))
                            .unwrap_or_else(|| String::new()),
                        metadata: config.metadata,
                        labels: config.labels,
                        annotations: config.annotations,
                        pod_sandbox_id: String::new(),
                    };

                    debug!(id = container.id, "found container");

                    containers.push(container);
                }
            }
        }

        Ok(containers)
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

    pub fn get_container(&self, id: &String) -> Result<ContainerConfig, anyhow::Error> {
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

    // add image and associate with given existing layers,
    // overwriting any existing image if needed
    pub fn add_image(
        &self,
        image: &RemoteImage,
        layers: &[ImageLayer],
    ) -> Result<bool, anyhow::Error> {
        let mut exists = false;

        let root = self.build_image_layers_path(&image);

        if root.exists() {
            exists = true;
        } else {
            fs::create_dir_all(&root)?;
        }

        for (idx, layer) in layers.iter().enumerate() {
            let mut file = File::create(root.join(idx.to_string()))?;
            file.write_all(layer.digest.as_bytes())?;
        }

        Ok(exists)
    }

    // remove given image from storage if possible
    pub fn remove_image(&self, _image: &RemoteImage) -> Result<(), anyhow::Error> {
        // TODO: actually remove only the provided image, not all of them
        fs::remove_dir_all(&self.images)?;
        fs::create_dir_all(&self.images)?;

        Ok(())
    }

    // get path to all images layers used by given image
    pub fn image_layers(&self, image: &RemoteImage) -> Result<Vec<PathBuf>, anyhow::Error> {
        let root = self.build_image_layers_path(image);
        if !root.exists() {
            return Err(anyhow::anyhow!("unknown image"));
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

    pub fn build_image_layers_path(&self, image: &RemoteImage) -> PathBuf {
        self.images
            .join(&image.domain)
            .join(&image.repository)
            .join("layers")
    }

    pub fn build_container_path(&self, id: &String) -> PathBuf {
        self.containers.join(id)
    }

    pub fn build_image_layer_path(&self, layer: &ImageLayer) -> PathBuf {
        self.layers.join(&layer.digest).join("layer.tar.gz")
    }
}
