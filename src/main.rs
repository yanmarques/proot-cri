use std::{
    collections::HashMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::header::{HeaderMap, HeaderValue};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use cri::runtime::{
    image_service_server::{ImageService, ImageServiceServer},
    runtime_service_server::{RuntimeService, RuntimeServiceServer},
    AttachRequest, AttachResponse, ContainerStatsRequest, ContainerStatsResponse,
    ContainerStatusRequest, ContainerStatusResponse, CreateContainerRequest,
    CreateContainerResponse, ExecRequest, ExecResponse, ExecSyncRequest, ExecSyncResponse,
    FilesystemIdentifier, FilesystemUsage, ImageFsInfoRequest, ImageFsInfoResponse,
    ImageStatusRequest, ImageStatusResponse, ListContainerStatsRequest, ListContainerStatsResponse,
    ListContainersRequest, ListContainersResponse, ListImagesRequest, ListImagesResponse,
    ListPodSandboxRequest, ListPodSandboxResponse, ListPodSandboxStatsRequest,
    ListPodSandboxStatsResponse, PodSandboxStatsRequest, PodSandboxStatsResponse,
    PodSandboxStatusRequest, PodSandboxStatusResponse, PortForwardRequest, PortForwardResponse,
    PullImageRequest, PullImageResponse, RemoveContainerRequest, RemoveContainerResponse,
    RemoveImageRequest, RemoveImageResponse, RemovePodSandboxRequest, RemovePodSandboxResponse,
    ReopenContainerLogRequest, ReopenContainerLogResponse, RunPodSandboxRequest,
    RunPodSandboxResponse, RuntimeCondition, RuntimeStatus, StartContainerRequest,
    StartContainerResponse, StatusRequest, StatusResponse, StopContainerRequest,
    StopContainerResponse, StopPodSandboxRequest, StopPodSandboxResponse,
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse, VersionRequest, VersionResponse,
};
use storage::{ImageLayer, RemoteImage, Storage};
use tracing::{debug, error, info};

mod cri;
mod storage;

const CRI_USER_AGENT: &'static str = "android-proot-cri";

/// Parse "WWW-Authenticate" header format
fn parse_www_authenticate(header: &str) -> Option<HashMap<String, String>> {
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

// file structure:
// images/
//  \_ alpine
//  |   \_ abcde....tar.gz
//  |   \_ abcde....tar.gz
//  \_ debian
//  |   \_ abcde....tar.gz
//  |   \_ abcde....tar.gz
//

#[derive(Debug)]
pub struct Runtime {
    storage: Storage,
}

impl Runtime {
    fn new(storage: Storage) -> Runtime {
        Runtime { storage }
    }
}

#[tonic::async_trait]
impl ImageService for Runtime {
    #[tracing::instrument]
    async fn image_fs_info(
        &self,
        _: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d,
            Err(err) => {
                error!(err = err.to_string(), "time went backwards?");
                return Err(Status::unavailable("server error"));
            }
        };

        let ts = match i64::try_from(now.as_secs()) {
            Ok(ts) => ts,
            Err(err) => {
                error!(
                    err = err.to_string(),
                    ts = now.as_secs(),
                    "timestamp was too long?"
                );
                return Err(Status::unavailable("server error"));
            }
        };

        let reply = ImageFsInfoResponse {
            image_filesystems: vec![FilesystemUsage {
                timestamp: ts,
                fs_id: Some(FilesystemIdentifier {
                    mountpoint: "/".to_string(),
                }),
                inodes_used: Some(cri::runtime::UInt64Value { value: 9999 }),
                used_bytes: Some(cri::runtime::UInt64Value {
                    value: 1024 * 1024 * 1024,
                }),
            }],
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn remove_image(
        &self,
        _: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let message = request.into_inner();
        let reference = message
            .image
            .ok_or_else(|| Status::invalid_argument("image"))?;

        let image = RemoteImage::parse(reference.image).map_err(|error| {
            error!(err = ?error, "failed to parse remote image");
            Status::invalid_argument("invalid image")
        })?;

        // TODO: add support for custom authentication
        if message.auth.is_some() {
            return Err(Status::invalid_argument("auth is not supported"));
        }

        let http = reqwest::Client::new();

        // @link https://github.com/openshift/docker-distribution/blob/master/docs/spec/api.md#api-version-check
        //
        //  ensure the remote registry is valid
        //
        let version_check = format!("https://{}/v2/", &image.domain);
        let resp = http
            .get(version_check)
            .headers(HeaderMap::from_iter([(
                reqwest::header::USER_AGENT,
                HeaderValue::from_static(CRI_USER_AGENT),
            )]))
            .send()
            .await
            .map_err(|error| {
                error!(err = ?error, "auth token request failed");
                Status::unavailable("authentication failed")
            })?;

        let version = resp
            .headers()
            .get("docker-distribution-api-version")
            .ok_or_else(|| Status::unavailable("remote is not a registry"))?;
        if version != "registry/2.0" {
            return Err(Status::internal("image unavailable"));
        }

        let auth_url = resp
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_www_authenticate)
            .and_then(|p| {
                if let Some(realm) = p.get("realm") {
                    return Some(realm.clone());
                }

                return None;
            })
            .ok_or_else(|| Status::invalid_argument("remote is not a registry"))?;

        //
        // fetch token without credentials
        //
        let query = &[
            ("service", &image.domain),
            ("scope", &format!("repository:{}:pull", &image.repository)),
        ];
        let resp = http
            .get(auth_url)
            .headers(HeaderMap::from_iter([(
                reqwest::header::USER_AGENT,
                HeaderValue::from_static(CRI_USER_AGENT),
            )]))
            .query(&query)
            .send()
            .await
            .map_err(|error| {
                error!(err = ?error, "failed to send authentication");
                Status::internal("image unavailable")
            })?;

        if !resp.status().is_success() {
            return Err(Status::internal("image unavailable"));
        }

        let data = resp.json::<serde_json::Value>().await.map_err(|error| {
            error!(err = ?error, "failed to parse json response from authentication");
            Status::internal("image unavailable")
        })?;

        let token = data
            .get("token")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| Status::internal("image unavailable"))?;

        debug!(token = token, "fetched token");

        let auth_header = HeaderValue::from_str(&format!("Bearer {}", token)).map_err(|error| {
            error!(err = ?error, "failed to fetch image manifests");
            Status::internal("image unavailable")
        })?;

        //
        //  fetch image manifests
        //
        let manifest_url = format!(
            "https://{}/v2/{}/manifests/{}",
            &image.domain, &image.repository, &image.tag
        );
        debug!(manifest_url = manifest_url);

        let resp = http
            .get(manifest_url)
            .headers(HeaderMap::from_iter([
                (
                    reqwest::header::USER_AGENT,
                    HeaderValue::from_static(CRI_USER_AGENT),
                ),
                (reqwest::header::AUTHORIZATION, auth_header.clone()),
                (
                    reqwest::header::ACCEPT,
                    HeaderValue::from_static(
                        "application/vnd.docker.distribution.manifest.v2+json",
                    ),
                ),
            ]))
            .send()
            .await
            .map_err(|error| {
                error!(err = ?error, "failed to fetch image manifests");
                Status::internal("image unavailable")
            })?;

        let status = resp.status();
        debug!(status = ?status, "manifests response");

        let data = resp.json::<serde_json::Value>().await.map_err(|error| {
            error!(err = ?error, "failed to parse json response from manifests");
            Status::internal("image unavailable")
        })?;

        let digest = data
            .get("manifests")
            .and_then(|v| v.as_array())
            .and_then(|m| {
                debug!(m = ?m, "found manifest");
                for manifest in m {
                    if let Some(arch) = manifest.get("platform").and_then(|p| p.get("architecture"))
                    {
                        // TODO: support multi-architectures
                        if arch == "amd64" {
                            return manifest
                                .get("digest")
                                .and_then(|d| d.as_str())
                                .map(String::from);
                        }
                    }
                }

                return None;
            })
            .ok_or_else(|| Status::internal("image unavailable"))?;

        let exists = self.storage.add_image_path(&image).map_err(|error| {
            error!(error = ?error, "failed to add image path");
            Status::internal("image unavailable")
        })?;

        debug!(exists = exists, "image already exists");

        //
        // fetch the layers
        //
        let layers_url = format!(
            "https://{}/v2/{}/manifests/{}",
            &image.domain, &image.repository, digest
        );
        debug!(layers_url = ?layers_url);

        let resp = http
            .get(layers_url)
            .headers(HeaderMap::from_iter([
                (
                    reqwest::header::USER_AGENT,
                    HeaderValue::from_static(CRI_USER_AGENT),
                ),
                (reqwest::header::AUTHORIZATION, auth_header.clone()),
                (
                    reqwest::header::ACCEPT,
                    HeaderValue::from_static("application/vnd.oci.image.manifest.v1+json"),
                ),
            ]))
            .send()
            .await
            .map_err(|error| {
                error!(err = ?error, "failed to fetch image layers");
                Status::internal("image unavailable")
            })?;

        let status = resp.status();
        debug!(status = ?status, "layers response");

        let data = resp.json::<serde_json::Value>().await.map_err(|error| {
            error!(err = ?error, "failed to parse json response from layers");
            Status::internal("image unavailable")
        })?;

        let layers = data
            .get("layers")
            .and_then(|v| v.as_array())
            .and_then(|m| {
                Some(
                    m.iter()
                        .map(|layer| {
                            Some(ImageLayer {
                                digest: layer
                                    .get("digest")
                                    .and_then(|d| d.as_str())
                                    .map(String::from)?,
                                size: layer.get("size").and_then(|s| s.as_u64())?,
                            })
                        })
                        .collect::<Vec<Option<ImageLayer>>>(),
                )
            })
            .ok_or_else(|| Status::internal("image unavailable"))?;

        for l in layers {
            if let Some(layer) = l {
                // TODO: add mutex
                if self.storage.has_image_layer(&layer) {
                    continue;
                }

                //
                // download layer
                //
                let layer_url = format!(
                    "https://{}/v2/{}/blobs/{}",
                    &image.domain, &image.repository, &layer.digest,
                );
                let resp = http
                    .get(layer_url)
                    .headers(HeaderMap::from_iter([
                        (
                            reqwest::header::USER_AGENT,
                            HeaderValue::from_static(CRI_USER_AGENT),
                        ),
                        (reqwest::header::AUTHORIZATION, auth_header.clone()),
                        (
                            reqwest::header::ACCEPT,
                            HeaderValue::from_static("application/octet-stream"),
                        ),
                    ]))
                    .send()
                    .await
                    .map_err(|error| {
                        error!(err = ?error, "failed to download image layer");
                        Status::internal("image unavailable")
                    })?;

                let buffer = resp.bytes().await.map_err(|error| {
                    error!(error = ?error, "failed downloading image layer");
                    Status::internal("image unavailable")
                })?;

                let _ = self
                    .storage
                    .add_image_layer(&layer, buffer)
                    .map_err(|error| {
                        error!(error = ?error, "failed inserting image layer");
                        Status::internal("image unavailable")
                    });
            }
        }

        let reply = PullImageResponse {
            image_ref: image.repository,
        };
        return Ok(Response::new(reply));

        // get token
        // https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/alpine:pull (fetch "token")
        // curl -v -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' -H 'Authorization: Bearer TOKEN' https://registry-1.docker.io/v2/library/alpine/manifests/latest
        // curl -v -H 'Accept: application/vnd.oci.image.manifest.v1+json' -H 'Authorization: Bearer TOKEN' https://registry-1.docker.io/v2/library/alpine/blobs/sha256:483f502c0e6aff6d80a807f25d3f88afa40439c29fdd2d21a0912e0f42db842a
        // curl -v -H 'Accept: application/octet-stream' -H 'Authorization: Bearer TOKEN' https://registry-1.docker.io/v2/library/alpine/blobs/sha256:LAYER_HASH
        // tar -zxf rootfs.tar.gz
    }

    #[tracing::instrument]
    async fn image_status(
        &self,
        _: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn list_images(
        &self,
        _: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let reply = ListImagesResponse { images: vec![] };

        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl RuntimeService for Runtime {
    #[tracing::instrument]
    async fn version(
        &self,
        _: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        let reply = VersionResponse {
            version: "v1".to_string(),
            runtime_name: "android-proot-cri".to_string(),
            runtime_version: "v0.1.0".to_string(),
            runtime_api_version: "0.1.0".to_string(),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let conditions = vec![
            RuntimeCondition {
                status: true,
                message: "RuntimeReady".to_string(),
                ..Default::default()
            },
            RuntimeCondition {
                status: true,
                message: "NetworkReady".to_string(),
                ..Default::default()
            },
            RuntimeCondition {
                status: true,
                message: "NetworkPluginReady".to_string(),
                ..Default::default()
            },
        ];

        let reply = StatusResponse {
            status: Some(RuntimeStatus { conditions }),
            info: HashMap::<String, String>::new(),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn update_runtime_config(
        &self,
        _: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        unimplemented!();
    }

    async fn list_pod_sandbox_stats(
        &self,
        _: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        unimplemented!();
    }

    async fn pod_sandbox_stats(
        &self,
        _: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        unimplemented!();
    }

    async fn list_container_stats(
        &self,
        _: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        unimplemented!();
    }

    async fn container_stats(
        &self,
        _: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        unimplemented!();
    }

    async fn port_forward(
        &self,
        _: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        unimplemented!();
    }

    async fn attach(&self, _: Request<AttachRequest>) -> Result<Response<AttachResponse>, Status> {
        unimplemented!();
    }

    async fn exec(&self, _: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        unimplemented!();
    }

    async fn exec_sync(
        &self,
        _: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        unimplemented!();
    }

    async fn reopen_container_log(
        &self,
        _: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        unimplemented!();
    }

    async fn update_container_resources(
        &self,
        _: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        unimplemented!();
    }

    async fn container_status(
        &self,
        _: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        unimplemented!();
    }

    async fn list_containers(
        &self,
        _: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let reply = ListContainersResponse { containers: vec![] };

        Ok(Response::new(reply))
    }

    async fn remove_container(
        &self,
        _: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        unimplemented!();
    }

    async fn stop_container(
        &self,
        _: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        unimplemented!();
    }

    async fn start_container(
        &self,
        _: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        unimplemented!();
    }

    async fn create_container(
        &self,
        _: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        unimplemented!();
    }

    async fn list_pod_sandbox(
        &self,
        _: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        unimplemented!();
    }

    async fn pod_sandbox_status(
        &self,
        _: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        unimplemented!();
    }

    async fn remove_pod_sandbox(
        &self,
        _: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        unimplemented!();
    }

    async fn stop_pod_sandbox(
        &self,
        _: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        unimplemented!();
    }

    async fn run_pod_sandbox(
        &self,
        _: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        unimplemented!();
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let root = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("Usage: android-proot-cri /path/to/storage"))?;

    let binding = std::env::current_dir()?;
    let cwd = binding
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("unable to get current working directory"))?;

    let parent = Path::new(cwd);

    std::fs::create_dir_all(parent)?;

    let path = parent.join("proot.sock");

    debug!(sock = path.to_str(), "starting to listen...");

    if path.exists() {
        std::fs::remove_file(&path)?;
    }

    let uds = UnixListener::bind(path.clone())?;

    info!(sock = path.to_str(), "started listener");

    let uds_stream = UnixListenerStream::new(uds);

    let storage = Storage::new(&root);
    storage.init()?;

    let runtime = Runtime::new(storage.clone());
    let image = Runtime::new(storage);

    Server::builder()
        .add_service(RuntimeServiceServer::new(runtime))
        .add_service(ImageServiceServer::new(image))
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}
