use std::{backtrace::Backtrace, collections::HashMap, path::Path, sync::Arc, time::Duration};

use clap::Parser;
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
    ImageFsInfoResponse, ImageSpec, ImageStatusRequest, ImageStatusResponse, KeyValue,
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
use storage::Storage;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter};
use utils::timestamp;

mod cri;
mod engine;
mod errors;
mod storage;
mod utils;

const SANDBOX_METADATA: &'static str = r#"
{
  "Metadata": {
    "AdditionalIPs": null,
    "CNIResult": {
      "DNS": [
        {},
        {}
      ],
      "Interfaces": {
        "cni0": {
          "IPConfigs": null,
          "Mac": "62:ee:10:4a:42:b7",
          "PciID": "",
          "Sandbox": "",
          "SocketPath": ""
        },
        "eth0": {
          "IPConfigs": [
            {
              "Gateway": "10.244.0.1",
              "IP": "10.244.0.5"
            }
          ],
          "Mac": "56:11:7e:6f:60:c3",
          "PciID": "",
          "Sandbox": "/var/run/netns/cni-44e45c20-3029-efdf-b4ab-f61567e3d1ba",
          "SocketPath": ""
        },
        "lo": {
          "IPConfigs": [
            {
              "Gateway": "",
              "IP": "127.0.0.1"
            },
            {
              "Gateway": "",
              "IP": "::1"
            }
          ],
          "Mac": "00:00:00:00:00:00",
          "PciID": "",
          "Sandbox": "/var/run/netns/cni-44e45c20-3029-efdf-b4ab-f61567e3d1ba",
          "SocketPath": ""
        },
        "veth39b447cd": {
          "IPConfigs": null,
          "Mac": "52:c0:5f:6c:ac:e0",
          "PciID": "",
          "Sandbox": "",
          "SocketPath": ""
        }
      },
      "Routes": [
        {
          "dst": "10.244.0.0/16"
        },
        {
          "dst": "0.0.0.0/0",
          "gw": "10.244.0.1"
        }
      ]
    },
    "Config": {
      "annotations": {
        "kubernetes.io/config.seen": "2025-02-09T09:35:24.639908563-03:00",
        "kubernetes.io/config.source": "api"
      },
      "dns_config": {
        "servers": [
          "10.139.1.1",
          "10.139.1.2",
          "10.139.1.1"
        ]
      },
      "hostname": "coredns-668d6bf9bc-hs68x",
      "labels": {
        "io.kubernetes.pod.name": "coredns-668d6bf9bc-hs68x",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "533d4d29-8861-4471-b1b7-6786c7e49861",
        "k8s-app": "kube-dns",
        "pod-template-hash": "668d6bf9bc"
      },
      "linux": {
        "cgroup_parent": "/kubepods/burstable/pod533d4d29-8861-4471-b1b7-6786c7e49861",
        "overhead": {},
        "resources": {
          "cpu_period": 100000,
          "cpu_shares": 102,
          "memory_limit_in_bytes": 178257920,
          "unified": {
            "memory.oom.group": "1"
          }
        },
        "security_context": {
          "namespace_options": {
            "pid": 1
          },
          "seccomp": {}
        }
      },
      "log_directory": "/var/log/pods/kube-system_coredns-668d6bf9bc-hs68x_533d4d29-8861-4471-b1b7-6786c7e49861",
      "metadata": {
        "name": "coredns-668d6bf9bc-hs68x",
        "namespace": "kube-system",
        "uid": "533d4d29-8861-4471-b1b7-6786c7e49861"
      },
      "port_mappings": [
        {
          "container_port": 53,
          "protocol": 1
        },
        {
          "container_port": 53
        },
        {
          "container_port": 9153
        }
      ]
    },
    "ID": "d222851da0713354bc13a4d4c7f762ac290901821621ca104d35691ff3488e01",
    "IP": "10.244.0.5",
    "Name": "coredns-668d6bf9bc-hs68x_kube-system_533d4d29-8861-4471-b1b7-6786c7e49861_0",
    "NetNSPath": "/var/run/netns/cni-44e45c20-3029-efdf-b4ab-f61567e3d1ba",
    "ProcessLabel": "",
    "RuntimeHandler": ""
  },
  "Version": "v1"
}
"#;

const RUNTIME_CONFIG: &'static str = r#"
{
    "cdiSpecDirs": [
      "/etc/cdi",
      "/var/run/cdi"
    ],
    "cni": {
      "binDir": "/opt/cni/bin",
      "confDir": "/etc/cni/net.d",
      "confTemplate": "",
      "ipPref": "",
      "maxConfNum": 1,
      "setupSerially": false,
      "useInternalLoopback": false
    },
    "containerd": {
      "defaultRuntimeName": "runc",
      "ignoreBlockIONotEnabledErrors": false,
      "ignoreRdtNotEnabledErrors": false,
      "runtimes": {
        "runc": {
          "ContainerAnnotations": [],
          "PodAnnotations": [],
          "baseRuntimeSpec": "",
          "cniConfDir": "",
          "cniMaxConfNum": 0,
          "io_type": "",
          "options": {
            "BinaryName": "",
            "CriuImagePath": "",
            "CriuWorkPath": "",
            "IoGid": 0,
            "IoUid": 0,
            "NoNewKeyring": false,
            "Root": "",
            "ShimCgroup": ""
          },
          "privileged_without_host_devices": false,
          "privileged_without_host_devices_all_devices_allowed": false,
          "runtimePath": "",
          "runtimeType": "io.containerd.runc.v2",
          "sandboxer": "podsandbox",
          "snapshotter": ""
        }
      }
    },
    "containerdEndpoint": "/run/containerd/containerd.sock",
    "containerdRootDir": "/var/lib/containerd",
    "device_ownership_from_security_context": false,
    "disableApparmor": false,
    "disableHugetlbController": true,
    "disableProcMount": false,
    "drainExecSyncIOTimeout": "0s",
    "enableCDI": true,
    "enableSelinux": false,
    "enableUnprivilegedICMP": true,
    "enableUnprivilegedPorts": true,
    "ignoreDeprecationWarnings": [],
    "ignoreImageDefinedVolumes": false,
    "maxContainerLogSize": 16384,
    "netnsMountsUnderStateDir": false,
    "restrictOOMScoreAdj": false,
    "rootDir": "/var/lib/containerd/io.containerd.grpc.v1.cri",
    "selinuxCategoryRange": 1024,
    "stateDir": "/run/containerd/io.containerd.grpc.v1.cri",
    "tolerateMissingHugetlbController": true,
    "unsetSeccompProfile": ""
}"#;

#[derive(Debug)]
pub struct RuntimeHandler {
    storage: Arc<Storage>,
    engine: Engine,
}

unsafe impl Send for RuntimeHandler {}
unsafe impl Sync for RuntimeHandler {}

#[derive(Debug)]
pub struct ImageHandler {
    storage: Storage,
}

impl RuntimeHandler {
    fn new(storage: Storage, engine: Engine) -> RuntimeHandler {
        RuntimeHandler {
            storage: Arc::new(storage),
            engine,
        }
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
        error!("remove image");

        Ok(Response::new(RemoveImageResponse {}))
    }

    #[tracing::instrument]
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let message = request.into_inner();
        // debug!("pull image: {:?}", message);
        let reference = message
            .image
            .ok_or_else(|| Status::invalid_argument("image"))?;

        // let image = RemoteImage::parse(&reference.image).map_err(|error| {
        //     error!(?error, "failed to parse remote image");
        //     Status::invalid_argument("invalid image")
        // })?;
        //
        // // TODO: add support for custom authentication
        // if message.auth.is_some() {
        //     return Err(Status::invalid_argument("auth is not supported"));
        // }
        //
        // let http = reqwest::Client::builder().build().map_err(|error| {
        //     error!(
        //         ?error,
        //         source = error.source().unwrap(),
        //         "failed to initialize client builder"
        //     );
        //     Status::internal("internal")
        // })?;
        //
        // // @link https://github.com/openshift/docker-distribution/blob/master/docs/spec/api.md#api-version-check
        // //
        // //  ensure the remote registry is valid
        // //
        // let version_check = format!("https://{}/v2/", &image.domain);
        // let resp = http
        //     .get(version_check)
        //     .headers(HeaderMap::from_iter([(
        //         reqwest::header::USER_AGENT,
        //         HeaderValue::from_static(CRI_USER_AGENT),
        //     )]))
        //     .send()
        //     .await
        //     .map_err(|error| {
        //         error!(?error, "auth token request failed");
        //         Status::unavailable("authentication failed")
        //     })?;
        //
        // let version = resp
        //     .headers()
        //     .get("docker-distribution-api-version")
        //     .ok_or_else(|| Status::unavailable("remote is not a registry"))?;
        // if version != "registry/2.0" {
        //     return Err(Status::internal("image unavailable"));
        // }
        //
        // let mut token = String::new();
        //
        // if let Some((auth_url, service)) = resp
        //     .headers()
        //     .get("www-authenticate")
        //     .and_then(|v| v.to_str().ok())
        //     .and_then(parse_www_authenticate)
        //     .and_then(|p| {
        //         if let Some(realm) = p.get("realm") {
        //             if let Some(service) = p.get("service") {
        //                 return Some((realm.clone(), service.clone()));
        //             }
        //         }
        //
        //         return None;
        //     })
        // {
        //     //
        //     // fetch token without credentials
        //     //
        //     let query = &[
        //         ("service", service),
        //         ("scope", format!("repository:{}:pull", &image.repository)),
        //     ];
        //     let resp = http
        //         .get(auth_url)
        //         .headers(HeaderMap::from_iter([(
        //             reqwest::header::USER_AGENT,
        //             HeaderValue::from_static(CRI_USER_AGENT),
        //         )]))
        //         .query(&query)
        //         .send()
        //         .await
        //         .map_err(|error| {
        //             error!(?error, "failed to send authentication");
        //             Status::internal("image unavailable")
        //         })?;
        //
        //     if !resp.status().is_success() {
        //         return Err(Status::internal("image unavailable"));
        //     }
        //
        //     let data = resp.json::<serde_json::Value>().await.map_err(|error| {
        //         error!(?error, "failed to parse json response from authentication");
        //         Status::internal("image unavailable")
        //     })?;
        //
        //     token = data
        //         .get("token")
        //         .and_then(|v| v.as_str())
        //         .map(String::from)
        //         .ok_or_else(|| Status::internal("image unavailable"))?;
        //
        //     debug!(token = token, "fetched token");
        // }
        //
        // let auth_header = HeaderValue::from_str(&format!("Bearer {}", token)).map_err(|error| {
        //     error!(?error, "failed to fetch image manifests");
        //     Status::internal("image unavailable")
        // })?;
        //
        // //
        // //  fetch image manifests
        // //
        // let manifest_url = format!(
        //     "https://{}/v2/{}/manifests/{}",
        //     &image.domain, &image.repository, &image.tag
        // );
        // debug!(manifest_url = manifest_url);
        //
        // let resp = http
        //     .get(manifest_url)
        //     .headers(HeaderMap::from_iter([
        //         (
        //             reqwest::header::USER_AGENT,
        //             HeaderValue::from_static(CRI_USER_AGENT),
        //         ),
        //         (reqwest::header::AUTHORIZATION, auth_header.clone()),
        //         (
        //             reqwest::header::ACCEPT,
        //             HeaderValue::from_static(
        //                 "application/vnd.docker.distribution.manifest.list.v2+json",
        //             ),
        //         ),
        //     ]))
        //     .send()
        //     .await
        //     .map_err(|error| {
        //         error!(?error, "failed to fetch image manifests");
        //         Status::internal("image unavailable")
        //     })?;
        //
        // let status = resp.status();
        // debug!(status = ?status, "manifests response");
        // if !status.is_success() {
        //     return Err(Status::invalid_argument("image"));
        // }
        //
        // let data = resp.json::<serde_json::Value>().await.map_err(|error| {
        //     error!(?error, "failed to parse json response from manifests");
        //     Status::internal("image unavailable")
        // })?;
        // debug!(?data, "manifests body");
        //
        // let digest = data
        //     .get("manifests")
        //     .and_then(|v| v.as_array())
        //     .and_then(|m| {
        //         debug!(length = m.len(), "manifests length");
        //         for manifest in m {
        //             if let Some(arch) = manifest.get("platform").and_then(|p| p.get("architecture"))
        //             {
        //                 // TODO: support multi-architectures
        //                 if arch == "amd64" {
        //                     return manifest
        //                         .get("digest")
        //                         .and_then(|d| d.as_str())
        //                         .map(String::from);
        //                 }
        //             }
        //         }
        //
        //         return None;
        //     })
        //     .ok_or_else(|| {
        //         error!("unable to find manifest");
        //         Status::internal("image unavailable")
        //     })?;
        //
        // if let Ok(Some(_)) = self.storage.get_image(&digest) {
        //     let reply = PullImageResponse { image_ref: digest };
        //
        //     return Ok(Response::new(reply));
        // }
        //
        // //
        // // fetch the layers
        // //
        // let layers_url = format!(
        //     "https://{}/v2/{}/manifests/{}",
        //     &image.domain, &image.repository, digest
        // );
        // debug!(layers_url = ?layers_url);
        //
        // let resp = http
        //     .get(layers_url)
        //     .headers(HeaderMap::from_iter([
        //         (
        //             reqwest::header::USER_AGENT,
        //             HeaderValue::from_static(CRI_USER_AGENT),
        //         ),
        //         (reqwest::header::AUTHORIZATION, auth_header.clone()),
        //         (
        //             reqwest::header::ACCEPT,
        //             HeaderValue::from_static(
        //                 "application/vnd.docker.distribution.manifest.v2+json",
        //             ),
        //         ),
        //     ]))
        //     .send()
        //     .await
        //     .map_err(|error| {
        //         error!(?error, "failed to fetch image layers");
        //         Status::internal("image unavailable")
        //     })?;
        //
        // let status = resp.status();
        // debug!(status = ?status, "layers response");
        // if !status.is_success() {
        //     return Err(Status::invalid_argument("image"));
        // }
        //
        // let data = resp.json::<serde_json::Value>().await.map_err(|error| {
        //     error!(?error, "failed to parse json response from layers");
        //     Status::internal("image unavailable")
        // })?;
        //
        // let layers = data
        //     .get("layers")
        //     .and_then(|v| v.as_array())
        //     .and_then(|m| {
        //         Some(
        //             m.iter()
        //                 .map(|layer| {
        //                     Some(ImageLayer {
        //                         digest: layer
        //                             .get("digest")
        //                             .and_then(|d| d.as_str())
        //                             .map(String::from)?,
        //                         size: layer.get("size").and_then(|s| s.as_u64())?,
        //                     })
        //                 })
        //                 .collect::<Vec<Option<ImageLayer>>>(),
        //         )
        //     })
        //     .ok_or_else(|| Status::internal("image unavailable"))?;
        //
        // let mut g_layers: Vec<ImageLayer> = Vec::with_capacity(layers.len());
        //
        // for l in layers {
        //     if let Some(layer) = l {
        //         // TODO: add mutex
        //         if self.storage.has_image_layer(&layer) {
        //             g_layers.push(layer);
        //             continue;
        //         }
        //
        //         //
        //         // download layer
        //         //
        //         let layer_url = format!(
        //             "https://{}/v2/{}/blobs/{}",
        //             &image.domain, &image.repository, &layer.digest,
        //         );
        //         let resp = http
        //             .get(layer_url)
        //             .headers(HeaderMap::from_iter([
        //                 (
        //                     reqwest::header::USER_AGENT,
        //                     HeaderValue::from_static(CRI_USER_AGENT),
        //                 ),
        //                 (reqwest::header::AUTHORIZATION, auth_header.clone()),
        //                 (
        //                     reqwest::header::ACCEPT,
        //                     HeaderValue::from_static("application/octet-stream"),
        //                 ),
        //             ]))
        //             .send()
        //             .await
        //             .map_err(|error| {
        //                 error!(?error, "failed to download image layer");
        //                 Status::internal("image unavailable")
        //             })?;
        //
        //         let buffer = resp.bytes().await.map_err(|error| {
        //             error!(?error, "failed downloading image layer");
        //             Status::internal("image unavailable")
        //         })?;
        //
        //         self.storage
        //             .add_image_layer(&layer, buffer)
        //             .map_err(|error| {
        //                 error!(?error, "failed inserting image layer");
        //                 Status::internal("image unavailable")
        //             })?;
        //
        //         g_layers.push(layer);
        //     }
        // }
        //
        let reference =
            oci_distribution::Reference::try_from(reference.image.clone()).map_err(|error| {
                error!(?error, ?reference, "failed to parse image reference");
                Status::invalid_argument("image")
            })?;

        let client = oci_distribution::Client::default();
        let image = client
            .pull(
                &reference,
                &oci_distribution::secrets::RegistryAuth::Anonymous,
                vec![
                    "application/vnd.oci.image.layer.v1.tar+gzip",
                    "application/vnd.docker.image.rootfs.diff.tar.gzip",
                ],
            )
            .await
            .map_err(|error| {
                error!(?error, ?reference, "failed to download image");
                Status::internal("image")
            })?;

        let image_id = utils::digest_to_image_id(&image);
        debug!(image_id, "found image id");

        if let Ok(Some(stored_image)) = self.storage.get_image(&image_id) {
            return Ok(Response::new(PullImageResponse {
                image_ref: stored_image
                    .image
                    .expect("bug? image in storage is incomplete")
                    .id,
            }));
        }

        for layer in image.layers.iter() {
            self.storage.add_image_layer(&layer).map_err(|error| {
                error!(?error, "failed inserting image layer");
                Status::internal("image unavailable")
            })?;
        }

        let image_id = self.storage.add_image(reference, image).map_err(|error| {
            error!(?error, "failed to store image");
            Status::internal("storage failed")
        })?;

        let reply = PullImageResponse {
            image_ref: image_id,
        };

        return Ok(Response::new(reply));
    }

    #[tracing::instrument]
    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let message = request.into_inner();
        // debug!(image = ?message.image, "image status");
        let image = message
            .image
            .ok_or_else(|| Status::invalid_argument("image"))?;

        let image = self.storage.get_image(&image.image).map_err(|error| {
            error!(?error, "failed to parse image");
            Status::invalid_argument("image")
        })?;

        let info = image
            .as_ref()
            .and_then(|image| {
                Some(HashMap::from([(
                    "info".to_string(),
                    format!(
                        "{{\"imageSpec\":{}}}",
                        String::from_utf8(image.raw_info.clone()).ok()?
                    ),
                )]))
            })
            .unwrap_or_else(|| HashMap::new());

        let reply = ImageStatusResponse {
            image: image.and_then(|image| image.image),
            info,
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn list_images(
        &self,
        _: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let reply = ListImagesResponse {
            images: self.storage.list_images().map_err(|error| {
                error!(?error, "failed to list images");
                Status::internal("storage failed")
            })?,
        };

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
        let reply = VersionResponse {
            version: "0.1.0".to_string(),
            runtime_api_version: "v1".to_string(),
            runtime_name: "containerd".to_string(),
            runtime_version: "v2.0.1".to_string(),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        // debug!("cri status");
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
            info: HashMap::from([("config".to_string(), RUNTIME_CONFIG.to_string())]),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn update_runtime_config(
        &self,
        request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        let message = request.into_inner();
        error!(
            requested_config = ?message.runtime_config,
            "update runtime config"
        );

        Ok(Response::new(UpdateRuntimeConfigResponse {}))
    }

    #[tracing::instrument]
    async fn list_pod_sandbox_stats(
        &self,
        _: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        error!("list pod sandbox stats");
        unimplemented!();
    }

    #[tracing::instrument]
    async fn pod_sandbox_stats(
        &self,
        _: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        error!("pod sandbox stats");
        unimplemented!();
    }

    #[tracing::instrument]
    async fn list_container_stats(
        &self,
        _: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
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
        error!("port forward");
        unimplemented!();
    }

    #[tracing::instrument]
    async fn attach(&self, _: Request<AttachRequest>) -> Result<Response<AttachResponse>, Status> {
        error!("attach");
        unimplemented!();
    }

    #[tracing::instrument]
    async fn exec(&self, _: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        error!("exec");
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
        request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        let message = request.into_inner();

        self.storage
            .reopen_container_log(&message.container_id)
            .map_err(|error| {
                error!(
                    ?error,
                    container_id = message.container_id,
                    "failed to reopen container log"
                );
                Status::internal("storage failed")
            })?;

        Ok(Response::new(ReopenContainerLogResponse {}))
    }

    async fn update_container_resources(
        &self,
        _: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        error!("update container resources");
        unimplemented!();
    }

    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let message = request.into_inner();

        if let Some(exitcode) = self.engine.try_wait(&message.container_id).await {
            self.storage
                .save_container_exitcode(&message.container_id, exitcode)
                .map_err(|error| {
                    error!(
                        ?error,
                        container_id = message.container_id,
                        "failed to save container exit code"
                    );
                    Status::internal("storage failed")
                })?;
        }

        let info = self
            .storage
            .get_container_config(&message.container_id)
            .map_err(|error| {
                error!(
                    ?error,
                    container_id = message.container_id,
                    "failed to list containers"
                );
                Status::internal("storage failed")
            })?;

        let (pod_sandbox_id, status) = self
            .storage
            .get_container_status(&message.container_id)
            .map_err(|error| {
                error!(
                    ?error,
                    container_id = message.container_id,
                    "failed to list containers"
                );
                Status::internal("storage failed")
            })?;

        // info!(
        //     container_id = message.container_id,
        //     status = status.as_ref().and_then(|s| Some(s.state.to_string())),
        //     "container status"
        // );

        // info!("{{\"sandboxID\":\"{}\",\"config\":{{\"runtimeSpec\":{{\"annotations\":{{\"io.kubernetes.cri.sandbox-id\":\"{}\"}}}}}}}}", pod_sandbox_id, pod_sandbox_id);
        let reply = ContainerStatusResponse {
            status,
            ..Default::default() // info: HashMap::from([(
                                 //     "info".to_string(),
                                 //     format!("{{\"sandboxID\":\"{}\",\"config\":{{\"runtimeSpec\":{{\"annotations\":{{\"io.kubernetes.cri.sandbox-id\":\"{}\"}}}}}}}}}}", pod_sandbox_id, pod_sandbox_id),
                                 // )]),
        };

        Ok(Response::new(reply))
    }

    // #[tracing::instrument]
    async fn list_containers(
        &self,
        _: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        self.engine
            .try_wait_all_containers()
            .await
            .iter()
            .for_each(|(id, exit_code)| {
                if let Err(error) = self.storage.save_container_exitcode(id, *exit_code) {
                    error!(?error, "failed to save container exit code");
                }
            });

        let containers = self.storage.list_containers().map_err(|error| {
            let bt = Backtrace::capture();
            error!(?error, ?bt, "failed to list containers");
            Status::internal("storage failed")
        })?;

        let reply = ListContainersResponse { containers };

        Ok(Response::new(reply))
    }

    async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        let message = request.into_inner();

        if let Err(error) = self.storage.remove_container(&message.container_id) {
            error!(?error, "failed to remove container");
        }

        Ok(Response::new(RemoveContainerResponse {}))
    }

    // #[tracing::instrument]
    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let message = request.into_inner();

        if self.storage.has_container(&message.container_id).is_none() {
            return Ok(Response::new(StopContainerResponse {}));
        }

        let timeout = if message.timeout > 0 {
            Some(Duration::from_secs(message.timeout.try_into().map_err(
                |error| {
                    error!(?error, timeout = ?message.timeout, "stop timeout is too large?");
                    Status::invalid_argument("stop timeout")
                },
            )?))
        } else {
            None
        };

        warn!(container_id = message.container_id, "stopping container");

        match self.engine.stop(&message.container_id, timeout).await {
            Ok(exitcode) => {
                if let Err(error) = self
                    .storage
                    .save_container_exitcode(&message.container_id, exitcode)
                {
                    error!(
                        ?error,
                        exitcode,
                        container = message.container_id,
                        "failed to save container exitcode"
                    );
                }
            }
            Err(error) => {
                debug!(
                    ?error,
                    container = message.container_id,
                    "failed to stop container"
                );
            }
        };

        Ok(Response::new(StopContainerResponse {}))
    }

    #[tracing::instrument]
    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        let message = request.into_inner();
        let data = self
            .storage
            .get_container_config(&message.container_id)
            .map_err(|error| {
                error!(?error, "container not in storage");
                Status::invalid_argument("unknown container")
            })?;

        let config = data.config.expect("bug? container is missing config");

        let image = config
            .image
            .as_ref()
            .ok_or_else(|| Status::internal("unknown container"))?;

        let layers = self.storage.image_layers(&image.image).map_err(|error| {
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
            .await
            .map_err(|error| {
                let bt = std::backtrace::Backtrace::capture();
                error!(?error, ?bt, "failed to start container");
                Status::internal("engine error")
            })?;

        if let Err(error) = self.storage.save_container_pid(&message.container_id, pid) {
            error!(?error, "failed to save container's pid");
            self.engine
                .stop(&message.container_id, None)
                .await
                .expect("could not save container pid, then could not stop the container");
            return Err(Status::internal("engine error"));
        }

        Ok(Response::new(StartContainerResponse {}))
    }

    // #[tracing::instrument]
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        let message = request.into_inner();
        let mut config = message
            .config
            .ok_or_else(|| Status::invalid_argument("config"))?;

        let image = config
            .image
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("image"))?;

        // ensure image exists
        let image_info = self
            .storage
            .get_image(&image.image)
            .map_err(|error| {
                error!(?error, ?image, "unknown image");
                Status::invalid_argument("image")
            })?
            .ok_or_else(|| Status::invalid_argument("image"))?;

        let build_config = image_info
            .build_config
            .expect("bug? missing build config from image");

        // use image default build configuration when container does not provide one
        if config.working_dir == "" {
            config.working_dir = build_config.working_dir.clone();
        }

        if config.command.is_empty() {
            config.command = build_config.entrypoint.clone();
        }

        if config.args.is_empty() {
            config.args = build_config.args.clone();
        }

        if config.envs.is_empty() && build_config.env.is_empty() {
            for env in build_config.env.iter() {
                let (key, value) = env
                    .split_once("=")
                    .expect("bug? image has wrong environment");

                config.envs.push(KeyValue {
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
        }

        let id = self
            .storage
            .add_container(&config, &message.pod_sandbox_id)
            .map_err(|error| {
                error!(?error, "failed storing container");
                Status::internal("storage unavailable")
            })?;

        let reply = CreateContainerResponse { container_id: id };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let mut sandboxes = self.storage.list_pod_sandboxes().map_err(|error| {
            error!(?error, "failed listing sandboxes");
            Status::internal("storage unavailable")
        })?;

        let message = request.into_inner();
        if let Some(filters) = message.filter {
            if filters.label_selector.len() > 0 {
                sandboxes.retain(|p| {
                    for (key, value) in filters.label_selector.iter() {
                        if p.labels.get(key) == Some(value) {
                            return true;
                        }
                    }

                    return false;
                });
            }
        }

        let reply = ListPodSandboxResponse { items: sandboxes };

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
            info: HashMap::from([("sandboxMetadata".to_string(), SANDBOX_METADATA.to_string())]),
        };

        Ok(Response::new(reply))
    }

    #[tracing::instrument]
    async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        let message = request.into_inner();
        self.storage
            .remove_pod_sandbox(&message.pod_sandbox_id)
            .map_err(|error| {
                error!(?error, "remove pod sandbox");
                Status::invalid_argument("pod sandbox")
            })?;

        Ok(Response::new(RemovePodSandboxResponse {}))
    }

    #[tracing::instrument]
    async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        let message = request.into_inner();

        self.storage
            .stop_pod_sandbox(&message.pod_sandbox_id)
            .map_err(|error| {
                error!(?error, "stop pod sandbox");
                Status::invalid_argument("pod sandbox")
            })?;

        if let Ok(containers) = self.storage.list_containers() {
            for container in containers {
                if container.pod_sandbox_id != message.pod_sandbox_id {
                    continue;
                }

                match self.engine.stop(&container.id, None).await {
                    Ok(exitcode) => {
                        if let Err(error) = self
                            .storage
                            .save_container_exitcode(&container.id, exitcode)
                        {
                            error!(?error, "failed to save container exit code");
                        }
                    }
                    Err(error) => {
                        error!(?error, "failed to stop container");
                    }
                };
            }
        }

        Ok(Response::new(StopPodSandboxResponse {}))
    }

    #[tracing::instrument]
    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let message = request.into_inner();

        let pod = self
            .storage
            .add_pod_sandbox(
                message
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
    tracing_subscriber::fmt::Subscriber::builder()
        .with_line_number(true)
        .with_env_filter(EnvFilter::from_default_env())
        .finish()
        .try_init()?;

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

    let storage = Storage::new(&args.storage)?;
    storage.init()?;

    let engine = Engine::new();

    let runtime = RuntimeHandler::new(storage.clone(), engine.clone());
    let image = ImageHandler::new(storage.clone());

    let mut ctrl_c = signal(SignalKind::interrupt())?;
    let mut term = signal(SignalKind::terminate())?;

    Server::builder()
        .tcp_keepalive(Some(Duration::from_secs(2)))
        .add_service(RuntimeServiceServer::new(runtime))
        .add_service(ImageServiceServer::new(image))
        .serve_with_incoming_shutdown(uds_stream, async {
            tokio::select! {
                _ = ctrl_c.recv() => {}
                _ = term.recv() => {}
            };
        })
        .await?;

    info!("stopping all containers...");

    engine.shutdown().await.iter().for_each(|(id, result)| {
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
