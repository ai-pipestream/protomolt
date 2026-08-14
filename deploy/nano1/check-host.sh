#!/usr/bin/env bash
# Read-only checks for the Nano1 native ARM64 builder and future GPU inference
# host. This script starts no container and changes no host state.
set -euo pipefail

say()  { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

say "architecture"
arch="$(uname -m)"
test "$arch" = aarch64 || fail "expected aarch64, found $arch"
printf 'architecture: %s\n' "$arch"

say "JetPack and L4T"
command -v dpkg-query >/dev/null || fail "dpkg-query is unavailable"
jetpack="$(dpkg-query -W -f='${Version}' nvidia-jetpack 2>/dev/null)" \
  || fail "nvidia-jetpack is not installed"
test -n "$jetpack" || fail "nvidia-jetpack has no version"
test -r /etc/nv_tegra_release || fail "/etc/nv_tegra_release is missing"
printf 'JetPack: %s\n' "$jetpack"
head -n 1 /etc/nv_tegra_release

say "GPU toolchain"
command -v nvidia-smi >/dev/null || fail "nvidia-smi is unavailable"
nvcc="$(command -v nvcc || true)"
if [ -z "$nvcc" ] && [ -x /usr/local/cuda/bin/nvcc ]; then
  nvcc=/usr/local/cuda/bin/nvcc
fi
test -n "$nvcc" || fail "nvcc is unavailable"
command -v nvpmodel >/dev/null || fail "nvpmodel is unavailable"
dpkg-query -W -f='TensorRT: ${Version}\n' tensorrt \
  || fail "TensorRT is not installed"
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
"$nvcc" --version | tail -n 1
nvpmodel -q | grep -q 'MAXN_SUPER' || fail "Nano1 is not in MAXN_SUPER mode"
printf 'power mode: MAXN_SUPER\n'

say "Docker and NVIDIA runtime"
command -v docker >/dev/null || fail "docker is unavailable"
docker version --format 'client={{.Client.Version}} server={{.Server.Version}}'
docker buildx version
docker info --format '{{json .Runtimes}}' | grep -q 'nvidia' \
  || fail "Docker has no NVIDIA runtime"
printf 'NVIDIA container runtime: configured\n'

say "PASS: Nano1 host"
