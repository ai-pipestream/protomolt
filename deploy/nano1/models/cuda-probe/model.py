"""Bounded TensorRT probe served through DJL's Python engine."""

import math
import time

from djl_python import Input
from djl_python import Output

from tensorrt_probe import TensorRtProbe


class CudaProbe:
    """Runs a fixed-shape TensorRT operation on one CUDA device."""

    def __init__(self):
        self.probe = None

    def initialize(self):
        self.probe = TensorRtProbe()

    def infer(self, inputs):
        request = inputs.get_as_json()
        if not isinstance(request, dict):
            return Output().error("request must be a JSON object", 400)

        values = request.get("values")
        if not isinstance(values, list) or len(values) != 16:
            return Output().error("values must contain exactly 16 numbers", 400)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in values
        ):
            return Output().error("values must contain only finite numbers", 400)

        started = time.perf_counter_ns()
        result = self.probe.run([float(value) for value in values])
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000

        response = {
            "backend": "TensorRT",
            "device": self.probe.device_name,
            "compute_capability": list(self.probe.compute_capability),
            "total_device_bytes": self.probe.total_device_bytes,
            "tensorrt_version": self.probe.tensorrt_version,
            "input": values,
            "output": result,
            "elapsed_ms": round(elapsed_ms, 3),
        }
        return Output().add_as_json(response)


_service = CudaProbe()


def handle(inputs: Input):
    """DJL Python engine entry point."""
    if _service.probe is None:
        _service.initialize()
    if inputs.is_empty():
        return None
    return _service.infer(inputs)
