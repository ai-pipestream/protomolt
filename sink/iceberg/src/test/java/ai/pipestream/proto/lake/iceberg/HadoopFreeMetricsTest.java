package ai.pipestream.proto.lake.iceberg;

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
 * Reading a Parquet footer for Iceberg column metrics must not drag in Hadoop, the same claim the
 * emitter's write path makes. The metrics read runs inside a classloader with every
 * {@code hadoop-client} jar removed (the test runtime otherwise carries one for the reader), so if
 * {@code ParquetUtil}'s footer read touches Hadoop this fails with the offending class named,
 * rather than the guarantee quietly rotting.
 */
class HadoopFreeMetricsTest {

    /**
     * Runs entirely inside the Hadoop-free classloader: emits Parquet with the descriptor-driven
     * emitter, reads its footer into Iceberg metrics, and returns the record count the footer
     * reports. Protobuf and Iceberg classes must come from the same loader as the emitter.
     */
    public static final class Runner {
        public static long run() throws Exception {
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
            byte[] bytes = ai.pipestream.proto.emit.parquet.ParquetEmitter
                    .toBytes(descriptor, List.of(message, message));
            org.apache.iceberg.Metrics metrics = IcebergMetrics.forParquet(
                    bytes, org.apache.iceberg.MetricsConfig.getDefault());
            return metrics.recordCount();
        }
    }

    @Test
    void theMetricsReadLoadsNoHadoopClasses() throws Exception {
        List<URL> hadoopFree = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            String jar = Path.of(entry).getFileName().toString();
            if (!jar.startsWith("hadoop-client")) { // keep parquet-hadoop; drop Hadoop itself
                hadoopFree.add(Path.of(entry).toUri().toURL());
            }
        }
        // The platform parent sees only the JDK; anything else must come from the filtered
        // classpath, so a Hadoop touch dies with NoClassDefFoundError.
        try (URLClassLoader isolated = new URLClassLoader(
                hadoopFree.toArray(URL[]::new), ClassLoader.getPlatformClassLoader())) {
            Class<?> runner = Class.forName(Runner.class.getName(), true, isolated);
            assertThat(runner.getClassLoader()).isSameAs(isolated);
            Method run = runner.getMethod("run");
            assertThat((long) run.invoke(null)).isEqualTo(2L);
        }
    }
}
