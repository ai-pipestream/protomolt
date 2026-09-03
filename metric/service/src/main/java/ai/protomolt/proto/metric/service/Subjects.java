package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.metric.spi.MetricRefusal;
import ai.protomolt.proto.metric.spi.MetricSubjectResolver;
import java.util.List;
import java.util.Map;

/**
 * One subject lookup for every service surface: the boot-static set first,
 * then the mount's resolver (rollup tables, which appear after boot by
 * design), then the unknown-subject refusal listing what is served. The
 * RPC and the verbs share this, so a subject resolves identically on
 * both.
 */
final class Subjects {

    private Subjects() {
    }

    static ServedMetricSubject find(Map<String, ServedMetricSubject> subjects,
            MetricSubjectResolver resolver, String name) {
        ServedMetricSubject served = subjects.get(name);
        if (served != null) {
            return served;
        }
        if (resolver != null) {
            MetricSubjectResolver.Resolved resolved = resolver.resolve(name);
            if (resolved != null) {
                return new ServedMetricSubject(resolved.mapping(),
                        Map.of(resolved.executor().backend(), resolved.executor()));
            }
        }
        throw new MetricRefusal(MetricRefusal.UNKNOWN_SUBJECT,
                "unknown mapping subject '" + name + "'; served subjects: "
                        + String.join(", ", subjects.keySet()),
                List.copyOf(subjects.keySet()));
    }
}
