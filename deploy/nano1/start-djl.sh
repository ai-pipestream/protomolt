#!/usr/bin/env bash
# Refuse to start unless TensorRT can execute on the Jetson GPU.
set -euo pipefail

export PYTHONPATH=/opt/ml/models/cuda-probe
python3 - <<'PYEOF'
import sys
from tensorrt_probe import TensorRtProbe

probe = TensorRtProbe()
try:
    result = probe.run([float(value) for value in range(16)])
    if result != [float(value + 2) for value in range(16)]:
        sys.exit("TensorRT probe returned an unexpected result")
    print(
        f"DJL GPU gate: device={probe.device_name} "
        f"compute={probe.compute_capability} TensorRT={probe.tensorrt_version}",
        flush=True,
    )
finally:
    probe.close()
PYEOF

exec djl-serving "$@"
