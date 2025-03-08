# PRoot-cri

A container runtime interface ([reference](https://kubernetes.io/docs/concepts/architecture/cri/)) that runs without root privileges in Android devices. Supported processor architectures include x86_64 (Android and Linux) and aarch64 (only Android; Linux support may be added later).

## Overview

The primary goal is to provide a Kubernetes-compatible container runtime that works without root privileges, particularly for Android userspace, where common CRIs like `containerd` don't work.

In terms of low-level implementation, to spawn isolated processes that act like normal containers without kernel support or root privileges, this runtime utilizes a fork of [proot](https://github.com/termux/proot) (which is also a fork of the original project and all credits go to them, see yourself: [proot-me/PRoot](https://github.com/proot-me/PRoot/)).

Find below known missing features:

1. View container logs; this will likely be added very soon. The existing workaround: `cat ./storage/containers/<container_id>/out.log`
2. Drop a shell inside the container; aka `exec -it`. There is specific workaround, but at least you can browse the filesystem of the container: `cd ./storage/containers/<container_id>/mounts/root/`

## Quick start

**Very important note**: This is a **pre-alpha** version. It may support running simple containers and a basic Kubernetes cluster. Expect missing features and bugs as development continues.

### 1. Download compiled binaries

Download appropriate binaries from the [releases page](https://github.com/yanmarques/proot-cri/releases/tag/0.0.1).

### 2. Extract

Replace the file name with the release of your architecture:

```bash
unzip proot-cri_<platform>.zip
```

The `proot-cri` binary will be available in your current working directory.

### 3. Run

What arguments are needed?

```bash
A container runtime based on PRoot

Usage: proot-cri [OPTIONS] --storage <STORAGE>

Options:
      --server <SERVER>    Path to unix socket to listen. Defaults to ./proot.sock
  -s, --storage <STORAGE>  Directory where container and images data will be stored
  -h, --help               Print help
  -V, --version            Print version
```

At a minimum, you need to specify a directory to store container data:

```bash
./proot-cri -s ./storage/
```

## Build from source

The instructions in the workflow files should guide you, [checkout them out](https://github.com/yanmarques/proot-cri/blob/main/.github/workflows/build.yml).

## Troubleshooting

Found a bug? Feel free to open a new issue. I trust you will be respectful and include detailed information.

