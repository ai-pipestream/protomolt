package ai.protomolt.proto.kafka.serde;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The class-name derivation protoc's naming rules imply, exercised against compiled descriptors
 * rather than real generated classes: {@code java_multiple_files}, an explicit outer classname,
 * the file-name camel-casing, and the {@code OuterClass} suffix when a type claims the file's
 * name. The end-to-end proof with real classes lives in {@link GeneratedClassSerdeTest}; these
 * pin the naming math itself, plus the quiet fallbacks that keep descriptor-set-only
 * deployments dynamic.
 */
class GeneratedMessagesTest {

    private static Descriptor compile(String path, String proto) throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(path, proto, "test").build());
        return compiled.descriptorFor(path).orElseThrow().getMessageTypes().get(0);
    }

    @Test
    void javaMultipleFilesMakesEachTopLevelTypeItsOwnClass() throws Exception {
        Descriptor foo = compile("acme/multi/foo.proto", """
                syntax = "proto3";
                package acme.multi;
                option java_package = "com.acme.generated";
                option java_multiple_files = true;
                message Foo { string a = 1; message Bar { string b = 1; } }
                """);
        assertThat(GeneratedMessages.binaryClassName(foo)).isEqualTo("com.acme.generated.Foo");
        assertThat(GeneratedMessages.binaryClassName(foo.findNestedTypeByName("Bar")))
                .isEqualTo("com.acme.generated.Foo$Bar");
    }

    /** No java options at all: the package is the proto's, the outer class the file's name. */
    @Test
    void defaultsComeFromTheProtoPackageAndFileName() throws Exception {
        Descriptor order = compile("acme/order_types.proto", """
                syntax = "proto3";
                package acme.orders;
                message Order { string id = 1; }
                """);
        assertThat(GeneratedMessages.binaryClassName(order))
                .isEqualTo("acme.orders.OrderTypes$Order");
    }

    @Test
    void anExplicitOuterClassnameWins() throws Exception {
        Descriptor thing = compile("acme/thing.proto", """
                syntax = "proto3";
                package acme.explicit;
                option java_outer_classname = "Everything";
                message Thing { string t = 1; }
                """);
        assertThat(GeneratedMessages.binaryClassName(thing))
                .isEqualTo("acme.explicit.Everything$Thing");
    }

    /** A message named after its file: protoc appends OuterClass to dodge the clash. */
    @Test
    void aTypeClaimingTheFileNamePushesTheOuterClassAside() throws Exception {
        Descriptor payment = compile("acme/payment.proto", """
                syntax = "proto3";
                package acme.pay;
                message Payment { string id = 1; }
                """);
        assertThat(GeneratedMessages.binaryClassName(payment))
                .isEqualTo("acme.pay.PaymentOuterClass$Payment");
    }

    @Test
    void javaMultipleFilesWithoutAJavaPackageUsesTheProtoPackage() throws Exception {
        Descriptor solo = compile("acme/solo.proto", """
                syntax = "proto3";
                package acme.solo;
                option java_multiple_files = true;
                message Solo { string s = 1; }
                """);
        assertThat(GeneratedMessages.binaryClassName(solo)).isEqualTo("acme.solo.Solo");
    }

    /** Turned off, every parse is dynamic — even for a type with a class on the classpath. */
    @Test
    void parsesDynamicallyWhenGeneratedClassesAreDisabled() throws Exception {
        Descriptor order = compile("acme/off/order.proto", """
                syntax = "proto3";
                package acme.off;
                message Order { string id = 1; }
                """);
        GeneratedMessages generated = new GeneratedMessages(
                List.of(order.getFile()), false, getClass().getClassLoader());
        byte[] payload = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), "A-1").build().toByteArray();

        Message parsed = generated.parse(order, payload);

        assertThat(parsed).isInstanceOf(DynamicMessage.class);
        assertThat(parsed.getField(order.findFieldByName("id"))).isEqualTo("A-1");
    }

    /** A type the packaged set does not declare cannot be matched to a class: stays dynamic. */
    @Test
    void staysDynamicForTypesOutsideThePackagedSet() throws Exception {
        Descriptor order = compile("acme/outside/order.proto", """
                syntax = "proto3";
                package acme.outside;
                message Order { string id = 1; }
                """);
        GeneratedMessages generated = new GeneratedMessages(
                List.of(), true, getClass().getClassLoader());
        byte[] payload = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), "A-2").build().toByteArray();

        assertThat(generated.parse(order, payload)).isInstanceOf(DynamicMessage.class);
    }
}
