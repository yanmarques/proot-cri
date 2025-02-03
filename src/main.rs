use std::{collections::HashMap, path::Path};

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use cri::{
    runtime_service_server::{RuntimeService, RuntimeServiceServer},
    AttachRequest, AttachResponse, ContainerStatsRequest, ContainerStatsResponse,
    ContainerStatusRequest, ContainerStatusResponse, CreateContainerRequest,
    CreateContainerResponse, ExecRequest, ExecResponse, ExecSyncRequest, ExecSyncResponse,
    ListContainerStatsRequest, ListContainerStatsResponse, ListContainersRequest,
    ListContainersResponse, ListPodSandboxRequest, ListPodSandboxResponse,
    ListPodSandboxStatsRequest, ListPodSandboxStatsResponse, PodSandboxStatsRequest,
    PodSandboxStatsResponse, PodSandboxStatusRequest, PodSandboxStatusResponse, PortForwardRequest,
    PortForwardResponse, RemoveContainerRequest, RemoveContainerResponse, RemovePodSandboxRequest,
    RemovePodSandboxResponse, ReopenContainerLogRequest, ReopenContainerLogResponse,
    RunPodSandboxRequest, RunPodSandboxResponse, RuntimeCondition, RuntimeStatus,
    StartContainerRequest, StartContainerResponse, StatusRequest, StatusResponse,
    StopContainerRequest, StopContainerResponse, StopPodSandboxRequest, StopPodSandboxResponse,
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse, VersionRequest, VersionResponse,
};
use tracing::{debug, info};

pub mod cri {
    tonic::include_proto!("runtime.v1");
}

#[derive(Debug, Default)]
pub struct Runtime {}

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
        unimplemented!();
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
    let subscriber = tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_level(true)
        .with_line_number(true)
        .with_file(true)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);

    let binding = std::env::current_dir()?;
    let cwd = match binding.to_str() {
        Some(cwd) => cwd,
        None => {
            return Err(anyhow::anyhow!("unable to get current working directory"));
        }
    };

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
    let runtime = Runtime::default();

    Server::builder()
        .add_service(RuntimeServiceServer::new(runtime))
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}
