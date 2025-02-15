import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

import static kernel.generated.delta_kernel_ffi_h.get_global_read_schema;
import static kernel.generated.delta_kernel_ffi_h.get_global_scan_state;

public class EngineContext {
    MemorySegment struct;
    // "context" argument, and then pass it back when calling a callback.
    EngineContext(SegmentAllocator allocator, MemorySegment scan, MemorySegment engine, MemorySegment tableRoot) {
        struct = allocator.allocate(layout());


        MemorySegment global_state = get_global_scan_state(scan);
        //MemorySegment logical_schema = get_global_logical_schema(global_state);
        MemorySegment read_schema = get_global_read_schema(global_state);
        //MemorySegment partition_cols = get_partition_list(global_state);

        var layout = layout();
        struct.set(delta_kernel_ffi_h.C_POINTER, 0, global_state);
        //struct.set((AddressLayout) layout, 8, logical_schema);
        struct.set(delta_kernel_ffi_h.C_POINTER, 16, read_schema);
        struct.set(delta_kernel_ffi_h.C_POINTER, 24, tableRoot);
        struct.set(delta_kernel_ffi_h.C_POINTER, 32, engine);
        //struct.set((AddressLayout) layout, 40, partition_cols);
    }

    MemoryLayout layout() {
        return MemoryLayout.structLayout(
                delta_kernel_ffi_h.C_POINTER.withName("global_state"),
                delta_kernel_ffi_h.C_POINTER.withName("logical_schema"),
                delta_kernel_ffi_h.C_POINTER.withName("read_schema"),
                delta_kernel_ffi_h.C_POINTER.withName("table_root"),
                delta_kernel_ffi_h.C_POINTER.withName("engine"),
                delta_kernel_ffi_h.C_POINTER.withName("partition_cols"),
                delta_kernel_ffi_h.C_POINTER.withName("partition_values")
        );
    }
    MemorySegment segment() {
        return struct;
    }


    private static class get_global_logical_schema {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        );

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("get_global_logical_schema");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }
    public static MemorySegment get_global_logical_schema(MemorySegment state) {
        var mh$ = get_global_logical_schema.HANDLE;
        try {
            return (MemorySegment)mh$.invokeExact(state);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    private static class get_partition_list {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        );

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("get_partition_list");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }
    public static MemorySegment get_partition_list(MemorySegment state) {
        var mh$ = get_partition_list.HANDLE;
        try {
            return (MemorySegment)mh$.invokeExact(state);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}
