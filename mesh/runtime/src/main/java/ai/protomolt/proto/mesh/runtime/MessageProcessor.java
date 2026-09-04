package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import com.google.protobuf.Message;

import java.util.List;

/** A descriptor-bound in-process processor. */
public interface MessageProcessor {

    /** The exact input and output schemas implemented by this processor. */
    ProcessorContract contract();

    /** Processes one protobuf message and returns zero or more protobuf messages in order. */
    List<? extends Message> process(ProcessorContext context, Message input) throws Exception;
}
