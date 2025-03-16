package kernel.oxidized_java;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.List;

import static kernel.generated.delta_kernel_ffi_h.snapshot;


public class RustSnapshot implements Snapshot {
    private final Arena arena;
    private final MemorySegment segment;
    public RustSnapshot(Arena arena, MemorySegment segment) {
        this.arena = arena;
        this.segment = segment;
    }

    public RustSnapshot(Arena arena, RustEngine engine, KernelStringSlice path) {
        var snapshotRes = snapshot(arena, path.segment(), engine.segment());
        var kernelResult = new KernelResult(snapshotRes);

        if (kernelResult.isErr()) {
            var err = kernelResult.err();
            throw new RuntimeException("Got error");
        }

        this.segment = kernelResult.ok();
        this.arena = arena;


        // Setup destructor
        segment.reinterpret(arena, snapshot -> {
            try {
                snapshot_cleanup.HANDLE.invokeExact(segment);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    public RustSnapshot(RustEngine engine, KernelStringSlice path) {
        this(Arena.ofAuto(), engine, path);
    }

    public MemorySegment segment() {
        return this.segment;
    }


    @Override
    public List<String> getPartitionColumnNames(Engine engine) {
        return List.of();
    }

    @Override
    public long getTimestamp(Engine engine) {
        // TODO
        return 0;
    }

    @Override
    public StructType getSchema(Engine engine) {
        return null;
    }

    @Override
    public ScanBuilder getScanBuilder(Engine engine) {
        return null;
    }

    private static class version {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_LONG_LONG,
                delta_kernel_ffi_h.C_POINTER
        );

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("version");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }

    @Override
    public long getVersion(Engine _engine) {
        var methodHandle = version.HANDLE;
        try {
            return (long)methodHandle.invokeExact(segment);
        } catch (Throwable err) {
            throw new AssertionError("should not reach here", err);
        }
    }


    private static class snapshot_table_root {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        );

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("snapshot_table_root");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }

    public String tableRoot() {
        var methodHandle = snapshot_table_root.HANDLE;
        try (Arena arenaConfined = Arena.ofConfined()){
            var rootSegment = (MemorySegment) methodHandle.invokeExact(segment, Utils.allocateStringFn(arenaConfined));
            return rootSegment.getString(0);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }


    private static class snapshot_cleanup {
        public static final FunctionDescriptor DESC = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("free_snapshot");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }
}
