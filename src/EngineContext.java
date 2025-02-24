import io.delta.kernel.types.StructType;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import static kernel.generated.delta_kernel_ffi_h.*;
import static kernel.generated.delta_kernel_ffi_h.string_slice_next;

public class EngineContext {
    MemorySegment struct;
    StructType logicalSchema;
    StructType readSchema;
    ArrayList<String> partitionColumns;
    String tableRoot;
    // "context" argument, and then pass it back when calling a callback.
    EngineContext(SegmentAllocator allocator, MemorySegment scan, MemorySegment engine, String tableRoot) {
        struct = allocator.allocate(layout());


        MemorySegment global_state = get_global_scan_state(scan);
        MemorySegment logical_schema = get_global_logical_schema(global_state);
        MemorySegment read_schema = get_global_read_schema(global_state);
        System.out.println("Going to visit");
        var visitor = new SchemaVisitor(allocator, Arena.ofConfined(), read_schema);
        System.out.println("Visitor: " + visitor.result);
        MemorySegment partition_cols = get_partition_columns(global_state);

        var layout = layout();
        struct.set(delta_kernel_ffi_h.C_POINTER, 0, global_state);
        struct.set(delta_kernel_ffi_h.C_POINTER, 8, logical_schema);
        struct.set(delta_kernel_ffi_h.C_POINTER, 16, read_schema);
//        struct.set(delta_kernel_ffi_h.C_POINTER, 24, tableRoot);
        struct.set(delta_kernel_ffi_h.C_POINTER, 32, engine);
        struct.set(delta_kernel_ffi_h.C_POINTER, 40, partition_cols);


        this.tableRoot = tableRoot;


        var logical_schema_visitor = new SchemaVisitor(Arena.ofConfined(), Arena.ofConfined(), logical_schema);
        this.logicalSchema = logical_schema_visitor.result;
        System.out.println("Logical schema: " + logicalSchema);

        var read_schema_visitor = new SchemaVisitor(Arena.ofConfined(), Arena.ofConfined(), read_schema);
        this.readSchema = read_schema_visitor.result;
        System.out.println("Read schema: " + this.readSchema);


        // bool string_slice_next(HandleStringSliceIterator data, NullableCvoid engine_context, void (*engine_visitor)(NullableCvoid, struct KernelStringSlice))
        // Get partition columns
//            void( * engine_visitor)(NullableCvoid, struct KernelStringSlice)
        var iter = new StringSliceIter();
        var descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, KernelStringSlice.layout());
        var handle = upcallHandle(StringSliceIter.class, "apply", descriptor).bindTo(iter);
        var handler = Linker.nativeLinker().upcallStub(handle, descriptor, Arena.ofConfined());
        for (; ; ) {
            boolean has_next = string_slice_next(partition_cols, MemorySegment.ofAddress(0), handler);
            if (!has_next) {
                System.out.println("Done iterating partition columns\n");
                break;
            }
        }
        this.partitionColumns = iter.list;
        System.out.println("Partition columns: " + this.partitionColumns);
    }

    public static MemoryLayout layout() {
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
    public MemorySegment segment() {
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

    private static class get_partition_columns {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        );

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("get_partition_columns");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }
    public static MemorySegment get_partition_columns(MemorySegment state) {
        var mh$ = get_partition_columns.HANDLE;
        try {
            return (MemorySegment)mh$.invokeExact(state);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}
