use std::{collections::HashMap, error::Error, path::Path, time::Duration};

use clap::Parser;
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::{
    net::UnixListener,
    signal::unix::{signal, SignalKind},
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use cri::runtime::{
    image_service_server::{ImageService, ImageServiceServer},
    runtime_service_server::{RuntimeService, RuntimeServiceServer},
    AttachRequest, AttachResponse, ContainerAttributes, ContainerStats, ContainerStatsRequest,
    ContainerStatsResponse, ContainerStatusRequest, ContainerStatusResponse,
    CreateContainerRequest, CreateContainerResponse, ExecRequest, ExecResponse, ExecSyncRequest,
    ExecSyncResponse, FilesystemIdentifier, FilesystemUsage, ImageFsInfoRequest,
    ImageFsInfoResponse, ImageSpec, ImageStatusRequest, ImageStatusResponse,
    ListContainerStatsRequest, ListContainerStatsResponse, ListContainersRequest,
    ListContainersResponse, ListImagesRequest, ListImagesResponse, ListPodSandboxRequest,
    ListPodSandboxResponse, ListPodSandboxStatsRequest, ListPodSandboxStatsResponse,
    PodSandboxStatsRequest, PodSandboxStatsResponse, PodSandboxStatusRequest,
    PodSandboxStatusResponse, PortForwardRequest, PortForwardResponse, PullImageRequest,
    PullImageResponse, RemoveContainerRequest, RemoveContainerResponse, RemoveImageRequest,
    RemoveImageResponse, RemovePodSandboxRequest, RemovePodSandboxResponse,
    ReopenContainerLogRequest, ReopenContainerLogResponse, RunPodSandboxRequest,
    RunPodSandboxResponse, RuntimeCondition, RuntimeStatus, StartContainerRequest,
    StartContainerResponse, StatusRequest, StatusResponse, StopContainerRequest,
    StopContainerResponse, StopPodSandboxRequest, StopPodSandboxResponse,
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse, VersionRequest, VersionResponse,
};
use engine::Engine;
use storage::{ImageLayer, RemoteImage, Storage};
use tracing::{debug, error, info};
use utils::{parse_www_authenticate, timestamp};

mod cri;
mod engine;
mod storage;
mod utils;

const CRI_USER_AGENT: &'static str = "android-proot-cri";

#[derive(Debug)]
pub struct RuntimeHandler {
    storage: Storage,
    engine: Engine,
}

#[derive(Debug)]
pub struct ImageHandler {
    storage: Storage,
}

impl RuntimeHandler {
    fn new(storage: Storage, engine: Engine) -> RuntimeHandler {
        RuntimeHandler { storage, engine }
    }
}

impl ImageHandler {
    fn new(storage: Storage) -> ImageHandler {
        ImageHandler { storage }
    }
}

#[tonic::async_trait]
impl ImageService for ImageHandler {
    #[tracing::instrument]
    async fn image_fs_info(
        &self,
        _: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        debug!("image fs info");
        let reply = ImageFsInfoResponse {
            image_filesystems: vec![FilesystemUsage {
                timestamp: timestamp().map_err(|error| {
                    error!(?error, "failed to get timestamp");
                    Status::internal("image")
                })?,
                fs_id: Some(FilesystemIdentifier {
                    mountpoint: self
                        .storage
                        .mountpoint()
                        .to_str()
                        .expect("mountpoint")
                        .to_string(),
                }),
                inodes_used: Some(cri::runtime::UInt64Value { value: 0 }),
                used_bytes: Some(cri::runtime::UInt64Value { value: 0 }),
            }],
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn remove_image(
        &self,
        _request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        // let message = request.into_inner();
        // let image = message
        //     .image
        //     .ok_or_else(|| Status::invalid_argument("image"))?;
        //
        // let image = RemoteImage::parse(image.image).map_err(|error| {
        //     error!(?error, "failed to parse image");
        //     Status::invalid_argument("image")
        // })?;
        //
        // self.storage.remove_image(&image).map_err(|error| {
        //     error!(?error, "failed to remove image");
        //     Status::internal("not removed")
        // })?;

        Ok(Response::new(RemoveImageResponse {}))
    }

    #[tracing::instrument]
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        debug!("pull image");
        let message = request.into_inner();
        let reference = message
            .image
            .ok_or_else(|| Status::invalid_argument("image"))?;

        let image = RemoteImage::parse(reference.image).map_err(|error| {
            error!(?error, "failed to parse remote image");
            Status::invalid_argument("invalid image")
        })?;

        // TODO: add support for custom authentication
        if message.auth.is_some() {
            return Err(Status::invalid_argument("auth is not supported"));
        }

        let http = reqwest::Client::builder().build().map_err(|error| {
            error!(
                ?error,
                source = error.source().unwrap(),
                "failed to initialize client builder"
            );
            Status::internal("internal")
        })?;

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
                error!(?error, "auth token request failed");
                Status::unavailable("authentication failed")
            })?;

        let version = resp
            .headers()
            .get("docker-distribution-api-version")
            .ok_or_else(|| Status::unavailable("remote is not a registry"))?;
        if version != "registry/2.0" {
            return Err(Status::internal("image unavailable"));
        }

        let mut token = String::new();

        if let Some((auth_url, service)) = resp
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_www_authenticate)
            .and_then(|p| {
                if let Some(realm) = p.get("realm") {
                    if let Some(service) = p.get("service") {
                        return Some((realm.clone(), service.clone()));
                    }
                }

                return None;
            })
        {
            //
            // fetch token without credentials
            //
            let query = &[
                ("service", service),
                ("scope", format!("repository:{}:pull", &image.repository)),
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
                    error!(?error, "failed to send authentication");
                    Status::internal("image unavailable")
                })?;

            if !resp.status().is_success() {
                return Err(Status::internal("image unavailable"));
            }

            let data = resp.json::<serde_json::Value>().await.map_err(|error| {
                error!(?error, "failed to parse json response from authentication");
                Status::internal("image unavailable")
            })?;

            token = data
                .get("token")
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| Status::internal("image unavailable"))?;

            debug!(token = token, "fetched token");
        }

        let auth_header = HeaderValue::from_str(&format!("Bearer {}", token)).map_err(|error| {
            error!(?error, "failed to fetch image manifests");
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
                error!(?error, "failed to fetch image manifests");
                Status::internal("image unavailable")
            })?;

        let status = resp.status();
        debug!(status = ?status, "manifests response");

        let data = resp.json::<serde_json::Value>().await.map_err(|error| {
            error!(?error, "failed to parse json response from manifests");
            Status::internal("image unavailable")
        })?;

        let digest = data
            .get("manifests")
            .and_then(|v| v.as_array())
            .and_then(|m| {
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
                error!(?error, "failed to fetch image layers");
                Status::internal("image unavailable")
            })?;

        let status = resp.status();
        debug!(status = ?status, "layers response");

        let data = resp.json::<serde_json::Value>().await.map_err(|error| {
            error!(?error, "failed to parse json response from layers");
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

        let mut g_layers: Vec<ImageLayer> = Vec::with_capacity(layers.len());

        for l in layers {
            if let Some(layer) = l {
                // TODO: add mutex
                if self.storage.has_image_layer(&layer) {
                    g_layers.push(layer);
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
                        error!(?error, "failed to download image layer");
                        Status::internal("image unavailable")
                    })?;

                let buffer = resp.bytes().await.map_err(|error| {
                    error!(?error, "failed downloading image layer");
                    Status::internal("image unavailable")
                })?;

                self.storage
                    .add_image_layer(&layer, buffer)
                    .map_err(|error| {
                        error!(?error, "failed inserting image layer");
                        Status::internal("image unavailable")
                    })?;

                g_layers.push(layer);
            }
        }

        let exists = self.storage.add_image(&image, &g_layers).map_err(|error| {
            error!(?error, "failed to store image");
            Status::internal("image unavailable")
        })?;
        debug!(exists = exists, "image exists");

        let reply = PullImageResponse {
            image_ref: image.repository,
        };

        return Ok(Response::new(reply));
    }

    #[tracing::instrument]
    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let message = request.into_inner();
        let image = message
            .image
            .ok_or_else(|| Status::invalid_argument("image"))?;

        let image = RemoteImage::parse(image.image).map_err(|error| {
            error!(?error, "failed to parse image");
            Status::invalid_argument("image")
        })?;

        let reply = ImageStatusResponse {
            image: Some(cri::runtime::Image {
                id: image.repository.clone(),
                size: 1,
                username: "root".to_string(),
                pinned: false,
                spec: Some(ImageSpec {
                    image: image.repository,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(Response::new(reply))
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
impl RuntimeService for RuntimeHandler {
    #[tracing::instrument]
    async fn version(
        &self,
        _: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        debug!("cri version");
        let reply = VersionResponse {
            version: "0.1.0".to_string(),
            runtime_api_version: "v1".to_string(),
            runtime_name: "android-proot-cri".to_string(),
            runtime_version: "v0.1.0".to_string(),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        debug!("cri status");
        let conditions = vec![
            RuntimeCondition {
                status: true,
                r#type: "RuntimeReady".to_string(),
                ..Default::default()
            },
            RuntimeCondition {
                status: true,
                r#type: "NetworkReady".to_string(),
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

    #[tracing::instrument]
    async fn list_pod_sandbox_stats(
        &self,
        _: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn pod_sandbox_stats(
        &self,
        _: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn list_container_stats(
        &self,
        _: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        debug!("container stats");
        let reply = ListContainerStatsResponse { stats: vec![] };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn container_stats(
        &self,
        _: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        debug!("container stats");

        let reply = ContainerStatsResponse { stats: None };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn port_forward(
        &self,
        _: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn attach(&self, _: Request<AttachRequest>) -> Result<Response<AttachResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn exec(&self, _: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
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
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let message = request.into_inner();

        let status = self
            .storage
            .get_container_status(&message.container_id)
            .map_err(|error| {
                error!(?error, "failed to list containers");
                Status::internal("storage failed")
            })?;

        let reply = ContainerStatusResponse {
            status: Some(status),
            ..Default::default()
        };

        Ok(Response::new(reply))
    }

    // #[tracing::instrument]
    async fn list_containers(
        &self,
        _: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let containers = self.storage.list_containers().map_err(|error| {
            error!(?error, "failed to list containers");
            Status::internal("storage failed")
        })?;

        let reply = ListContainersResponse { containers };

        Ok(Response::new(reply))
    }

    async fn remove_container(
        &self,
        _: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let message = request.into_inner();
        let _ = self
            .storage
            .get_container_config(&message.container_id)
            .map_err(|error| {
                error!(?error, "container not in storage");
                Status::invalid_argument("unknown container")
            })?;

        let timeout = if message.timeout > 0 {
            Some(Duration::from_secs(message.timeout.try_into().map_err(
                |error| {
                    error!(?error, "stop timeout is too large?");
                    Status::invalid_argument("stop timeout")
                },
            )?))
        } else {
            None
        };

        let exitcode = self
            .engine
            .stop(&message.container_id, timeout)
            .map_err(|error| {
                let bt = std::backtrace::Backtrace::capture();
                error!(?error, ?bt, "failed to start container");
                Status::internal("engine error")
            })?;

        self.storage
            .save_container_exitcode(&message.container_id, exitcode)
            .map_err(|error| {
                error!(?error, "failed to save container's exitcode");
                Status::internal("storage error")
            })?;

        Ok(Response::new(StopContainerResponse {}))
    }

    #[tracing::instrument]
    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        debug!("start container");
        let message = request.into_inner();
        let config = self
            .storage
            .get_container_config(&message.container_id)
            .map_err(|error| {
                error!(?error, "container not in storage");
                Status::invalid_argument("unknown container")
            })?;

        let image = {
            let image_spec = config
                .image
                .clone()
                .ok_or_else(|| Status::internal("unknown container"))?;

            RemoteImage::parse(image_spec.image).map_err(|error| {
                error!(?error, "failed parsing image");
                Status::internal("unknown container")
            })?
        };

        let layers = self.storage.image_layers(&image).map_err(|error| {
            error!(?error, "image layers unavailable");
            Status::invalid_argument("layers error")
        })?;

        let (pid, _) = self
            .engine
            .spawn(
                &message.container_id,
                &config,
                self.storage.build_container_path(&message.container_id),
                layers,
            )
            .map_err(|error| {
                let bt = std::backtrace::Backtrace::capture();
                error!(?error, ?bt, "failed to start container");
                Status::internal("engine error")
            })?;

        self.storage
            .save_container_pid(&message.container_id, pid)
            .map_err(|error| {
                error!(?error, "failed to save container's pid");
                self.engine
                    .stop(&message.container_id, None)
                    .expect("could not save container pid, then could not stop the container");
                Status::internal("engine error")
            })?;

        Ok(Response::new(StartContainerResponse {}))
    }

    #[tracing::instrument]
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        let message = request.into_inner();
        let config = message
            .config
            .ok_or_else(|| Status::invalid_argument("config"))?;

        ImageHandler::new(self.storage.clone())
            .pull_image(Request::new(PullImageRequest {
                image: config.image.clone(),
                auth: None,
                sandbox_config: None,
            }))
            .await?;

        let id = self.storage.add_container(&config).map_err(|error| {
            error!(?error, "failed storing container");
            Status::internal("storage unavailable")
        })?;

        let reply = CreateContainerResponse { container_id: id };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn list_pod_sandbox(
        &self,
        _: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let reply = ListPodSandboxResponse {
            items: self.storage.list_pod_sandboxes().map_err(|error| {
                error!(?error, "failed listing sandboxes");
                Status::internal("storage unavailable")
            })?,
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let message = request.into_inner();

        let status = self
            .storage
            .get_pod_sandbox_status(&message.pod_sandbox_id)
            .map_err(|error| {
                error!(?error, "failed finding sandbox");
                Status::internal("storage unavailable")
            })?;

        let reply = PodSandboxStatusResponse {
            status: Some(status),
            ..Default::default()
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn remove_pod_sandbox(
        &self,
        _: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        unimplemented!();
    }

    #[tracing::instrument]
    async fn stop_pod_sandbox(
        &self,
        _: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        // TODO: stop containers associated with pod
        Ok(Response::new(StopPodSandboxResponse {}))
    }

    #[tracing::instrument]
    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        debug!("creating pod sandbox");
        let message = request.into_inner();

        let pod = self
            .storage
            .add_pod_sandbox(
                &message
                    .config
                    .ok_or_else(|| Status::invalid_argument("config"))?,
            )
            .map_err(|error| {
                error!(?error, "failed listing sandboxes");
                Status::internal("storage unavailable")
            })?;

        // let reply = RunPodSandboxResponse {
        //     pod_sandbox_id: random_id().map_err(|error| {
        //         error!(?error, "rng failed");
        //         Status::internal("unable to process")
        //     })?,
        // };
        let reply = RunPodSandboxResponse {
            pod_sandbox_id: pod,
        };

        Ok(Response::new(reply))
    }
}

/// A container runtime based on PRoot
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to unix socket to listen. Defaults to ./proot.sock
    #[arg(long)]
    server: Option<String>,

    /// Directory where container and images data will be stored
    #[arg(short, long)]
    storage: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let server_path = args
        .server
        .and_then(|s| Some(Path::new(&s).to_path_buf()))
        .unwrap_or_else(|| {
            let binding = std::env::current_dir().expect("unable to get current directory");
            let cwd = binding
                .to_str()
                .expect("unable to get current working directory");

            Path::new(cwd).join("proot.sock")
        });

    if server_path.exists() {
        std::fs::remove_file(&server_path)?;
    }

    let uds = UnixListener::bind(server_path.clone())?;

    info!(sock = server_path.to_str(), "started listener");

    let uds_stream = UnixListenerStream::new(uds);

    let storage = Storage::new(&args.storage);
    storage.init()?;

    let engine = Engine::new();

    let runtime = RuntimeHandler::new(storage.clone(), engine.clone());
    let image = ImageHandler::new(storage.clone());

    let server = Server::builder()
        .add_service(RuntimeServiceServer::new(runtime))
        .add_service(ImageServiceServer::new(image))
        .serve_with_incoming(uds_stream);

    let mut ctrl_c = signal(SignalKind::interrupt())?;
    let mut term = signal(SignalKind::terminate())?;

    tokio::select! {
        result = server => {
            if let Err(error) = result {
                error!(?error, "server returned error")
            }
        }
        _ = ctrl_c.recv() => {}
        _ = term.recv() => {}
    };

    info!("stopping all containers...");

    engine.shutdown().iter().for_each(|(id, result)| {
        match result {
            Ok(exitcode) => {
                let _ = storage
                    .save_container_exitcode(&id, exitcode.clone())
                    .inspect_err(|error| {
                        error!(id, ?error, "failed to save container exit code");
                    });
            }
            Err(error) => {
                error!(id, ?error, "failed to stop container");
            }
        };
    });

    info!("exiting...");

    Ok(())
}
