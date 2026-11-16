package ai.protomolt.proto.jobs.service;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Set;

/**
 * Which message types print as something other than an object in proto3 JSON, and how such a
 * message is carried where the action contract needs an object.
 *
 * <p>The job verbs carry a run's input, a unary step response and a result as JSON objects.
 * Most messages print as one. The well-known types below print as a string, a number, a
 * boolean, an array or any value, so in those places they are carried as
 * {@code {"value": <their proto3 JSON>}}. Stream items are carried as they print.
 */
final class Proto3JsonShape {

    /** The single key of the object that carries a message which does not print as one. */
    static final String VALUE_KEY = "value";

    private static final Set<String> NOT_OBJECTS = Set.of(
            "google.protobuf.DoubleValue",
            "google.protobuf.FloatValue",
            "google.protobuf.Int64Value",
            "google.protobuf.UInt64Value",
            "google.protobuf.Int32Value",
            "google.protobuf.UInt32Value",
            "google.protobuf.BoolValue",
            "google.protobuf.StringValue",
            "google.protobuf.BytesValue",
            "google.protobuf.Timestamp",
            "google.protobuf.Duration",
            "google.protobuf.FieldMask",
            "google.protobuf.ListValue",
            "google.protobuf.Value");

    private Proto3JsonShape() {
    }

    /** True when every message of {@code type} prints as a JSON object. */
    static boolean printsAsObject(Descriptor type) {
        return !NOT_OBJECTS.contains(type.getFullName());
    }
}
