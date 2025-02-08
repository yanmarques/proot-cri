use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("container `{0}` does not exist or was not found")]
    ContainerNotFound(String),
}
