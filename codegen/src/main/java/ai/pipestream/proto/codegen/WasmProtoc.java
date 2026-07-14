package ai.pipestream.proto.codegen;

import com.dylibso.chicory.compiler.MachineFactoryCompiler;
import com.dylibso.chicory.runtime.ByteArrayMemory;
import com.dylibso.chicory.runtime.ImportMemory;
import com.dylibso.chicory.runtime.ImportValues;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Machine;
import com.dylibso.chicory.wasi.WasiOptions;
import com.dylibso.chicory.wasi.WasiPreview1;
import com.dylibso.chicory.wasm.Parser;
import com.dylibso.chicory.wasm.WasmModule;
import com.dylibso.chicory.wasm.types.MemoryLimits;
import com.google.protobuf.compiler.PluginProtos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

/**
 * protoc's code generators running as WebAssembly inside the JVM: a {@code CodeGeneratorRequest}
 * goes in on stdin, a {@code CodeGeneratorResponse} comes back on stdout, exactly the protoc
 * plugin protocol, with no native toolchain anywhere. The bundled module carries protoc's Java, Kotlin,
 * Python, C++, C#, Ruby, PHP, and Objective-C generators plus the grpc-java plugin.
 *
 * <p>The WebAssembly module and the execution approach come from
 * <a href="https://github.com/ai-pipestream/protobuf4j">protobuf4j</a> (Apache-2.0), which
 * compiles upstream protobuf to Wasm via Chicory. The module is compiled to JVM bytecode once
 * per process on first use.</p>
 */
public final class WasmProtoc {

    /** Generators embedded in the bundled protoc module. */
    public enum Plugin {
        JAVA("java"),
        KOTLIN("kotlin"),
        GRPC_JAVA("grpc-java"),
        PYTHON("python"),
        CPP("cpp"),
        CSHARP("csharp"),
        RUBY("ruby"),
        PHP("php"),
        OBJC("objc");

        private final String wrapperArg;

        Plugin(String wrapperArg) {
            this.wrapperArg = wrapperArg;
        }

        public String wrapperArg() {
            return wrapperArg;
        }
    }

    private static final int INITIAL_MEMORY_PAGES = 1024;

    private WasmProtoc() {
    }

    // The 2.6 MB module parse and its bytecode compilation happen once, lazily.
    private static final class Compiled {
        static final WasmModule MODULE = load();
        static final Function<Instance, Machine> MACHINE_FACTORY =
                MachineFactoryCompiler.compile(MODULE);

        private static WasmModule load() {
            try (InputStream in = WasmProtoc.class.getResourceAsStream("protoc-wrapper-v4.wasm")) {
                if (in == null) {
                    throw new IllegalStateException("protoc-wrapper-v4.wasm missing from classpath");
                }
                return Parser.parse(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Runs one generator over the request; protoc-reported problems arrive in the response's error field. */
    public static PluginProtos.CodeGeneratorResponse run(
            Plugin plugin, PluginProtos.CodeGeneratorRequest request) {
        try (ByteArrayInputStream stdin = new ByteArrayInputStream(request.toByteArray());
             ByteArrayOutputStream stdout = new ByteArrayOutputStream();
             ByteArrayOutputStream stderr = new ByteArrayOutputStream()) {
            WasiOptions wasiOptions = WasiOptions.builder()
                    .withStdin(stdin)
                    .withStdout(stdout)
                    .withStderr(stderr)
                    .withArguments(List.of("protoc-wrapper", plugin.wrapperArg()))
                    .build();
            try (WasiPreview1 wasi = WasiPreview1.builder().withOptions(wasiOptions).build()) {
                ImportValues imports = ImportValues.builder()
                        .addFunction(wasi.toHostFunctions())
                        .addMemory(new ImportMemory("env", "memory", new ByteArrayMemory(
                                new MemoryLimits(INITIAL_MEMORY_PAGES, MemoryLimits.MAX_PAGES, true))))
                        .build();
                Instance.builder(Compiled.MODULE)
                        .withImportValues(imports)
                        .withMachineFactory(Compiled.MACHINE_FACTORY)
                        .withMemoryFactory(ByteArrayMemory::new)
                        .build();
            } catch (RuntimeException e) {
                String detail = stderr.size() > 0
                        ? ": " + stderr.toString(StandardCharsets.UTF_8).strip()
                        : "";
                throw new IllegalStateException(
                        "protoc " + plugin.wrapperArg() + " generator failed" + detail, e);
            }
            return PluginProtos.CodeGeneratorResponse.parseFrom(stdout.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
