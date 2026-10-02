package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Services and their methods. Methods are matched by name within their service, so a method has
 * no wire-level identity to preserve the way a field number does: dropping one breaks the callers
 * that still invoke it, which is a forward concern rather than a backward one.
 */
final class ServiceDiff {

    private ServiceDiff() {
    }

    static void diffAll(DescriptorIndex oldIndex, DescriptorIndex newIndex,
                        List<SchemaChange> changes) {
        for (Map.Entry<String, ServiceDescriptorProto> entry : oldIndex.services.entrySet()) {
            String fqn = entry.getKey();
            ServiceDescriptorProto newService = newIndex.services.get(fqn);
            if (newService == null) {
                changes.add(new SchemaChange(ChangeRules.SERVICE_REMOVED, fqn,
                        "service " + fqn, "",
                        "Service " + fqn + " was removed; old clients still call it.",
                        Set.of(Impact.WIRE_FORWARD, Impact.SOURCE)));
            } else {
                diffService(fqn, entry.getValue(), newService, changes);
            }
        }
        for (String fqn : newIndex.services.keySet()) {
            if (!oldIndex.services.containsKey(fqn)) {
                changes.add(new SchemaChange(ChangeRules.SERVICE_ADDED, fqn,
                        "", "service " + fqn,
                        "Service " + fqn + " was added.", DiffImpacts.INFO));
            }
        }
    }

    private static void diffService(String fqn, ServiceDescriptorProto oldService,
                                    ServiceDescriptorProto newService,
                                    List<SchemaChange> changes) {
        Map<String, MethodDescriptorProto> newMethods = new LinkedHashMap<>();
        for (MethodDescriptorProto method : newService.getMethodList()) {
            newMethods.put(method.getName(), method);
        }
        Set<String> oldNames = new HashSet<>();
        for (MethodDescriptorProto oldMethod : oldService.getMethodList()) {
            oldNames.add(oldMethod.getName());
            String path = fqn + "." + oldMethod.getName();
            MethodDescriptorProto newMethod = newMethods.get(oldMethod.getName());
            if (newMethod == null) {
                changes.add(new SchemaChange(ChangeRules.METHOD_REMOVED, path,
                        DescriptorSnippets.methodSnippet(oldMethod), "",
                        "Method " + path + " was removed; old clients still call it.",
                        Set.of(Impact.WIRE_FORWARD, Impact.SOURCE)));
                continue;
            }
            diffMethod(path, oldMethod, newMethod, changes);
        }
        for (MethodDescriptorProto newMethod : newService.getMethodList()) {
            if (!oldNames.contains(newMethod.getName())) {
                changes.add(new SchemaChange(ChangeRules.METHOD_ADDED, fqn + "." + newMethod.getName(),
                        "", DescriptorSnippets.methodSnippet(newMethod),
                        "Method " + fqn + "." + newMethod.getName() + " was added.",
                        DiffImpacts.INFO));
            }
        }
    }

    private static void diffMethod(String path, MethodDescriptorProto oldMethod,
                                   MethodDescriptorProto newMethod, List<SchemaChange> changes) {
        String oldInput = DescriptorSnippets.stripDot(oldMethod.getInputType());
        String newInput = DescriptorSnippets.stripDot(newMethod.getInputType());
        if (!oldInput.equals(newInput)) {
            changes.add(new SchemaChange(ChangeRules.METHOD_REQUEST_TYPE_CHANGED, path,
                    DescriptorSnippets.methodSnippet(oldMethod),
                    DescriptorSnippets.methodSnippet(newMethod),
                    "Method " + path + " changed request type from " + oldInput + " to "
                            + newInput + ".",
                    DiffImpacts.WIRE_BOTH_SOURCE));
        }
        String oldOutput = DescriptorSnippets.stripDot(oldMethod.getOutputType());
        String newOutput = DescriptorSnippets.stripDot(newMethod.getOutputType());
        if (!oldOutput.equals(newOutput)) {
            changes.add(new SchemaChange(ChangeRules.METHOD_RESPONSE_TYPE_CHANGED, path,
                    DescriptorSnippets.methodSnippet(oldMethod),
                    DescriptorSnippets.methodSnippet(newMethod),
                    "Method " + path + " changed response type from " + oldOutput + " to "
                            + newOutput + ".",
                    DiffImpacts.WIRE_BOTH_SOURCE));
        }
        if (oldMethod.getClientStreaming() != newMethod.getClientStreaming()
                || oldMethod.getServerStreaming() != newMethod.getServerStreaming()) {
            changes.add(new SchemaChange(ChangeRules.METHOD_STREAMING_CHANGED, path,
                    DescriptorSnippets.methodSnippet(oldMethod),
                    DescriptorSnippets.methodSnippet(newMethod),
                    "Method " + path + " changed its streaming shape.",
                    DiffImpacts.WIRE_BOTH_SOURCE));
        }
    }
}
