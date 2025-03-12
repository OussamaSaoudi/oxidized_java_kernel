import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import static io.delta.kernel.types.StructField.METADATA_ROW_INDEX_COLUMN;
import static kernel.generated.delta_kernel_ffi_h.*;
import static kernel.generated.delta_kernel_ffi_h.string_slice_next;

public class EngineContext {
    StructType logicalSchema;
    StructType readSchema;
    ArrayList<String> partitionColumns;
    String tableRoot;
    Queue<RustScanFileRow> queue;
    RustEngine engine;
    MemorySegment globalScanState;
    Engine javaEngine;

    // "context" argument, and then pass it back when calling a callback.
    EngineContext(RustEngine engine, RustScan scan, String tableRoot, RustSnapshot snapshot) {
        // Set the engine so we can use it in the Scan Row Callback

        this.engine = engine;
        // Get global scan state
        globalScanState = get_global_scan_state(scan.segment());

        // Set the table root
        this.tableRoot = tableRoot;

        // Read the Read Schema
        MemorySegment read_schema = get_global_read_schema(globalScanState);
        try (Arena visitor_arena = Arena.ofConfined()) {
            var visitor = new SchemaVisitor(visitor_arena, read_schema);
            readSchema = visitor.result.add(METADATA_ROW_INDEX_COLUMN);
        }

        // Read the Logical Schema
        MemorySegment logical_schema = get_global_logical_schema(globalScanState);
        try (Arena visitorArena = Arena.ofConfined()) {
            var logical_schema_visitor = new SchemaVisitor(visitorArena, logical_schema);
            this.logicalSchema = logical_schema_visitor.result;
        }

        javaEngine = DefaultEngine.create(new Configuration());

        // Get Partition Columns
        MemorySegment partition_cols = get_partition_columns(snapshot.segment());
        var iter = new StringSliceIter();
        var descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, KernelStringSlice.layout());
        var handle = upcallHandle(StringSliceIter.class, "apply", descriptor).bindTo(iter);
        var handler = Linker.nativeLinker().upcallStub(handle, descriptor, Arena.ofConfined());
        for (; ; ) {
            boolean has_next = string_slice_next(partition_cols, MemorySegment.ofAddress(0), handler);
            if (!has_next) {
                break;
            }
        }
        this.partitionColumns = iter.list;

        // Initialize the queue to empty.
        this.queue = new LinkedList<>();
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
