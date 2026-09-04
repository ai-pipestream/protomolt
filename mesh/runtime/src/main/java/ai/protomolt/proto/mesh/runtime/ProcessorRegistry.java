package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.ProcessorContracts;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Exact-contract registry shared by local and remote processor endpoints. */
public final class ProcessorRegistry {

    private final DescriptorRegistry descriptors;
    private final Map<String, ProcessorInvoker> processors = new LinkedHashMap<>();

    public ProcessorRegistry(DescriptorRegistry descriptors) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
    }

    /** Registers an in-process processor. Duplicate ids are always refused. */
    public synchronized void register(MessageProcessor processor) {
        Objects.requireNonNull(processor, "processor");
        ProcessorContract contract = validateContract(processor.contract());
        registerValidated(new LocalInvoker(processor, contract));
    }

    /** Registers an invocation endpoint, including a remote dispatcher. */
    public synchronized void register(ProcessorInvoker invoker) {
        Objects.requireNonNull(invoker, "invoker");
        ProcessorContract contract = validateContract(invoker.contract());
        registerValidated(new CheckedInvoker(invoker, contract));
    }

    /** Registers a remote endpoint once and accepts byte-identical reconnects. */
    public synchronized void registerOrVerify(ProcessorInvoker invoker) {
        Objects.requireNonNull(invoker, "invoker");
        ProcessorContract contract = validateContract(invoker.contract());
        ProcessorInvoker existing = processors.get(contract.getProcessorId());
        if (existing == null) {
            processors.put(contract.getProcessorId(), new CheckedInvoker(invoker, contract));
            return;
        }
        if (!ProcessorContracts.exactMatch(existing.contract(), contract)) {
            throw new IllegalArgumentException("processor id is already registered with a "
                    + "different exact contract: " + contract.getProcessorId());
        }
    }

    public synchronized Optional<ProcessorInvoker> find(String processorId) {
        return Optional.ofNullable(processors.get(processorId));
    }

    public synchronized Map<String, ProcessorContract> contracts() {
        Map<String, ProcessorContract> result = new LinkedHashMap<>();
        processors.forEach((id, invoker) -> result.put(id, invoker.contract()));
        return Map.copyOf(result);
    }

    private void registerValidated(ProcessorInvoker invoker) {
        String id = invoker.contract().getProcessorId();
        if (processors.putIfAbsent(id, invoker) != null) {
            throw new IllegalArgumentException("processor id is already registered: " + id);
        }
    }

    private ProcessorContract validateContract(ProcessorContract contract) {
        contract = ProcessorContracts.canonical(
                Objects.requireNonNull(contract, "processor contract"));
        if (contract.getProcessorId().isBlank()) {
            throw new IllegalArgumentException("processor contract requires processor_id");
        }
        if (!contract.hasInputSchema()) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " requires input_schema");
        }
        RuntimeSchemas.resolve(descriptors, contract.getInputSchema());
        if (contract.getOutputSchemasCount() == 0) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " requires at least one output schema");
        }
        Set<DescriptorIdentity> outputs = new LinkedHashSet<>();
        for (SchemaReference output : contract.getOutputSchemasList()) {
            RuntimeSchemas.resolve(descriptors, output);
            if (!outputs.add(RuntimeSchemas.identity(output))) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " repeats output schema " + output.getTypeName());
            }
        }
        if (contract.getMaxOutputs() < 1) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " requires a positive max_outputs");
        }
        return contract;
    }

    private final class LocalInvoker implements ProcessorInvoker {
        private final MessageProcessor processor;
        private final ProcessorContract contract;

        private LocalInvoker(MessageProcessor processor, ProcessorContract contract) {
            this.processor = processor;
            this.contract = contract;
        }

        @Override
        public ProcessorContract contract() {
            return contract;
        }

        @Override
        public ProcessorInvocationResult invoke(ProcessorInvocation invocation) throws Exception {
            requireInput(contract, invocation.inputMessage());
            List<? extends Message> raw = processor.process(
                    invocation.context(), invocation.inputMessage());
            if (raw == null) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned a null output list");
            }
            if (raw.size() > contract.getMaxOutputs()) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned " + raw.size() + " outputs, exceeding max_outputs "
                        + contract.getMaxOutputs());
            }
            List<TypedPayload> outputs = new ArrayList<>(raw.size());
            for (Message message : raw) {
                Objects.requireNonNull(message,
                        "processor " + contract.getProcessorId() + " returned a null output");
                requireOutput(contract, message.getDescriptorForType().getFullName(),
                        DescriptorIdentity.of(message.getDescriptorForType()));
                outputs.add(RuntimeSchemas.pack(message));
            }
            return ProcessorInvocationResult.local(outputs);
        }
    }

    private final class CheckedInvoker implements ProcessorInvoker {
        private final ProcessorInvoker delegate;
        private final ProcessorContract contract;

        private CheckedInvoker(ProcessorInvoker delegate, ProcessorContract contract) {
            this.delegate = delegate;
            this.contract = contract;
        }

        @Override
        public ProcessorContract contract() {
            return contract;
        }

        @Override
        public ProcessorInvocationResult invoke(ProcessorInvocation invocation) throws Exception {
            requireInput(contract, invocation.inputMessage());
            ProcessorInvocationResult result = Objects.requireNonNull(delegate.invoke(invocation),
                    "processor invocation result");
            if (result.outputs().size() > contract.getMaxOutputs()) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned " + result.outputs().size()
                        + " outputs, exceeding max_outputs " + contract.getMaxOutputs());
            }
            for (TypedPayload output : result.outputs()) {
                Message parsed = RuntimeSchemas.unpack(descriptors, output);
                requireOutput(contract, output.getSchema().getTypeName(),
                        DescriptorIdentity.of(parsed.getDescriptorForType()));
            }
            return result;
        }

        @Override
        public InvocationSettlement recoverSettlement(String deliveryId) {
            return delegate.recoverSettlement(deliveryId);
        }
    }

    private static void requireInput(ProcessorContract contract, Message input) {
        DescriptorIdentity expected = RuntimeSchemas.identity(contract.getInputSchema());
        DescriptorIdentity actual = DescriptorIdentity.of(input.getDescriptorForType());
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " expects input " + expected + " but received " + actual);
        }
    }

    private static void requireOutput(
            ProcessorContract contract, String typeName, DescriptorIdentity actual) {
        boolean accepted = contract.getOutputSchemasList().stream()
                .map(RuntimeSchemas::identity)
                .anyMatch(actual::equals);
        if (!accepted) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " returned undeclared output schema " + typeName + " ("
                    + actual.fingerprint() + ")");
        }
    }
}
