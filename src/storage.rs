use std::{
    fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use prost::bytes::Buf;
use tracing::debug;

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
            fs::create_dir_all(&self.root)?;
        }

        if !self.images.exists() {
            fs::create_dir_all(&self.images)?;
        }

        if !self.layers.exists() {
            fs::create_dir_all(&self.layers)?;
        }

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

        let root = self
            .images
            .join(&image.domain)
            .join(&image.repository)
            .join("layers");

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

    pub fn build_image_layer_path(&self, layer: &ImageLayer) -> PathBuf {
        self.layers.join(&layer.digest).join("layer.tar.gz")
    }
}
