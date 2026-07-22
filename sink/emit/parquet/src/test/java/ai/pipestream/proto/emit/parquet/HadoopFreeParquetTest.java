package ai.pipestream.proto.emit.parquet;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The emitter's headline dependency claim, enforced: writing Parquet loads zero Hadoop
 * classes. The writer runs inside a classloader whose classpath has every Hadoop jar
 * removed and whose parent is the platform loader — if anything on the write path touches
 * Hadoop (the way parquet's default CodecFactory materializes a Configuration), this test
 * fails with the offending class named instead of the claim quietly rotting.
 */
class HadoopFreeParquetTest {

    /**
     * Runs entirely inside the Hadoop-free classloader: builds its own descriptor and
     * messages (protobuf classes must come from the same loader as the emitter) and
     * returns the written file's size.
     */
    public static final class Runner {
        public static int run() throws Exception {
            var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("iso.proto").setPackage("iso").setSyntax("proto3")
                    .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto
                            .newBuilder()
                            .setName("Row")
                            .addField(com.google.protobuf.DescriptorProtos
                                    .FieldDescriptorProto.newBuilder()
                                    .setName("name").setNumber(1)
                                    .setType(com.google.protobuf.DescriptorProtos
                                            .FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(com.google.protobuf.DescriptorProtos
                                            .FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .build();
            var descriptor = com.google.protobuf.Descriptors.FileDescriptor
                    .buildFrom(file, new com.google.protobuf.Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("Row");
            var message = com.google.protobuf.DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("name"), "isolated")
                    .build();
            byte[] bytes = ParquetEmitter.toBytes(descriptor, List.of(message, message));
            return bytes.length;
        }
    }

    @Test
    void theWritePathLoadsNoHadoopClasses() throws Exception {
        List<URL> hadoopFree = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path")
                .split(File.pathSeparator)) {
            String jar = Path.of(entry).getFileName().toString();
            if (!jar.startsWith("hadoop-client")) { // keep parquet-hadoop; drop Hadoop itself
                hadoopFree.add(Path.of(entry).toUri().toURL());
            }
        }
        // The platform parent sees only the JDK; anything else must come from the
        // filtered classpath, so a Hadoop touch dies with NoClassDefFoundError.
        try (URLClassLoader isolated = new URLClassLoader(
                hadoopFree.toArray(URL[]::new), ClassLoader.getPlatformClassLoader())) {
            Class<?> runner = Class.forName(Runner.class.getName(), true, isolated);
            assertThat(runner.getClassLoader()).isSameAs(isolated);
            Method run = runner.getMethod("run");
            int size = (int) run.invoke(null);
            assertThat(size).isGreaterThan(100);
        }
    }
}
