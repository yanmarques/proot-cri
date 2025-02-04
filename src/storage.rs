use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use prost::bytes::Buf;

/// Image layer
#[derive(Debug)]
pub struct ImageLayer {
    /// Digest (e.g., "sha256:abcdef..")
    pub digest: String,
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
            Some((domain, name)) => {
                if name.contains("/") {
                    (domain.to_string(), name.to_string())
                } else {
                    (domain.to_string(), format!("library/{}", name))
                }
            }
            None => ("registry-1.docker.io".to_string(), name.to_string()),
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
    images: PathBuf,
    layers: PathBuf,
}

impl Storage {
    pub fn new(root: &str) -> Storage {
        let root_path = Path::new(root);
        let images = root_path.join("images");
        let layers = root_path.join("layers");
        Storage {
            root: root_path.to_path_buf(),
            images,
            layers,
        }
    }

    /// initialize storage
    pub fn init(&self) -> Result<(), anyhow::Error> {
        if !self.root.exists() {
            std::fs::create_dir_all(&self.root)?;
        }

        if !self.images.exists() {
            std::fs::create_dir_all(&self.images)?;
        }

        if !self.layers.exists() {
            std::fs::create_dir_all(&self.layers)?;
        }

        Ok(())
    }

    // register image if not exists
    pub fn add_image_path(&self, image: &RemoteImage) -> Result<bool, anyhow::Error> {
        let mut exists = false;

        let root = self.images.join(&image.domain);
        if !root.exists() {
            std::fs::create_dir_all(&root)?;
        }

        let img_root = root.join(&image.repository);
        if img_root.exists() {
            exists = true;
        } else {
            std::fs::create_dir_all(&img_root)?;
        }

        Ok(exists)
    }

    /// check whether the layer exists
    pub fn has_image_layer(&self, layer: &ImageLayer) -> bool {
        let layer_fs = self.layers.join(&layer.digest).join("layer.tar.gz");

        layer_fs.exists()
    }

    /// check whether the layer exists
    pub fn add_image_layer(
        &self,
        layer: &ImageLayer,
        buffer: impl Buf,
    ) -> Result<(), anyhow::Error> {
        let layer_dir = self.layers.join(&layer.digest);
        let layer_path = layer_dir.join("layer.tar.gz");

        if !layer_dir.exists() {
            std::fs::create_dir_all(&layer_dir)?;
        }

        let mut file = File::create(layer_path)?;
        file.write_all(buffer.chunk())?;
        Ok(())
    }
}
