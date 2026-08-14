"""Small fixed-shape TensorRT engine used for live GPU acceptance."""

import numpy as np
import tensorrt as trt
from cuda.bindings import runtime as cudart


def _cuda_value(call):
    error, *values = call
    if error != cudart.cudaError_t.cudaSuccess:
        raise RuntimeError(f"CUDA operation failed with {error}")
    if len(values) == 1:
        return values[0]
    return values


class TensorRtProbe:
    """Adds two to a 16-value tensor using a TensorRT CUDA engine."""

    def __init__(self):
        error, properties = cudart.cudaGetDeviceProperties(0)
        if error != cudart.cudaError_t.cudaSuccess:
            raise RuntimeError("CUDA device 0 is unavailable; CPU inference is prohibited")

        self.device_name = properties.name.decode("utf-8")
        self.compute_capability = (properties.major, properties.minor)
        self.total_device_bytes = properties.totalGlobalMem
        self.tensorrt_version = trt.__version__
        self.logger = trt.Logger(trt.Logger.WARNING)
        self.engine = self._build_engine()
        self.context = self.engine.create_execution_context()
        self.stream = _cuda_value(cudart.cudaStreamCreate())
        self.byte_count = 16 * np.dtype(np.float32).itemsize
        self.device_input = _cuda_value(cudart.cudaMalloc(self.byte_count))
        self.device_output = _cuda_value(cudart.cudaMalloc(self.byte_count))
        if not self.context.set_tensor_address("input", self.device_input):
            raise RuntimeError("failed to bind TensorRT input")
        if not self.context.set_tensor_address("output", self.device_output):
            raise RuntimeError("failed to bind TensorRT output")

    def _build_engine(self):
        builder = trt.Builder(self.logger)
        network = builder.create_network(
            1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH)
        )
        input_tensor = network.add_input("input", trt.float32, (1, 16))
        constant_values = np.full((1, 16), 2.0, dtype=np.float32)
        constant = network.add_constant((1, 16), constant_values).get_output(0)
        output = network.add_elementwise(
            input_tensor, constant, trt.ElementWiseOperation.SUM
        ).get_output(0)
        output.name = "output"
        network.mark_output(output)
        plan = builder.build_serialized_network(
            network, builder.create_builder_config()
        )
        if plan is None:
            raise RuntimeError("TensorRT failed to build the CUDA probe")
        engine = trt.Runtime(self.logger).deserialize_cuda_engine(plan)
        if engine is None:
            raise RuntimeError("TensorRT failed to load the CUDA probe")
        return engine

    def run(self, values):
        source = np.asarray(values, dtype=np.float32).reshape(1, 16)
        target = np.empty_like(source)
        _cuda_value(
            cudart.cudaMemcpyAsync(
                self.device_input,
                source.ctypes.data,
                self.byte_count,
                cudart.cudaMemcpyKind.cudaMemcpyHostToDevice,
                self.stream,
            )
        )
        if not self.context.execute_async_v3(stream_handle=self.stream):
            raise RuntimeError("TensorRT execution failed")
        _cuda_value(
            cudart.cudaMemcpyAsync(
                target.ctypes.data,
                self.device_output,
                self.byte_count,
                cudart.cudaMemcpyKind.cudaMemcpyDeviceToHost,
                self.stream,
            )
        )
        _cuda_value(cudart.cudaStreamSynchronize(self.stream))
        return target.reshape(16).tolist()

    def close(self):
        if getattr(self, "device_input", None) is not None:
            _cuda_value(cudart.cudaFree(self.device_input))
            self.device_input = None
        if getattr(self, "device_output", None) is not None:
            _cuda_value(cudart.cudaFree(self.device_output))
            self.device_output = None
        if getattr(self, "stream", None) is not None:
            _cuda_value(cudart.cudaStreamDestroy(self.stream))
            self.stream = None
