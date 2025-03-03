import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import static kernel.generated.delta_kernel_ffi_h.*;
import static kernel.generated.delta_kernel_ffi_h.string_slice_next;

public class EngineContext {
    StructType logicalSchema;
    StructType readSchema;
    ArrayList<String> partitionColumns;
    String tableRoot;
    CloseableIterator<FilteredColumnarBatch> queue;

    // "context" argument, and then pass it back when calling a callback.
    EngineContext(MemorySegment scan, String tableRoot) {
        // Get global scan state
        MemorySegment global_state = get_global_scan_state(scan);

        // Set the table root
        this.tableRoot = tableRoot;

        // Read the Read Schema
        MemorySegment read_schema = get_global_read_schema(global_state);
        try (Arena visitor_arena = Arena.ofConfined()) {
            var visitor = new SchemaVisitor(visitor_arena, read_schema);
            readSchema = visitor.result;
        }

        // Read the Logical Schema
        MemorySegment logical_schema = get_global_logical_schema(global_state);
        try (Arena visitorArena = Arena.ofConfined()) {
            var logical_schema_visitor = new SchemaVisitor(visitorArena, logical_schema);
            this.logicalSchema = logical_schema_visitor.result;
        }

        // Get Partition Columns
        MemorySegment partition_cols = get_partition_columns(global_state);
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


        System.out.println("Logical schema: " + logicalSchema);

        System.out.println("Read schema: " + this.readSchema);


        // Initialize the queue to empty.
        this.queue = new CloseableIterator<FilteredColumnarBatch>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public FilteredColumnarBatch next() {
                return null;
            }

            @Override
            public void close() throws IOException {
            }
        };
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
            return (MemorySegment) mh$.invokeExact(state);
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
            return (MemorySegment) mh$.invokeExact(state);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}
