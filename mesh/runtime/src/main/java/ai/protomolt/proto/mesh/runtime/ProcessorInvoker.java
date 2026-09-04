package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;

/** Common invocation seam for in-process and demand-driven remote processors. */
public interface ProcessorInvoker {

    ProcessorContract contract();

    ProcessorInvocationResult invoke(ProcessorInvocation invocation) throws Exception;
}
