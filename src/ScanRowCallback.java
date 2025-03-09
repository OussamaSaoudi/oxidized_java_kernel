import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import kernel.generated.*;
import kernel.generated.KernelStringSlice;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.foreign.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static kernel.generated.delta_kernel_ffi_h.*;

public class ScanRowCallback implements CScanCallback.Function {

    EngineContext context;

    public ScanRowCallback(EngineContext context) {
        this.context = context;
    }

    @Override
    public void apply(MemorySegment engine_context, MemorySegment path, long size, MemorySegment stats, MemorySegment dv_info, MemorySegment expression, MemorySegment partition_map) {
        var strLen = KernelStringSlice.len(path);
        var strPtr = KernelStringSlice.ptr(path);
        var pathStr = strPtr.getString(0).substring(0, (int) strLen);
        pathStr = context.tableRoot + pathStr;
        try (Arena arena = Arena.ofConfined()) {
            // Get FileStatus
//            FileStatus fileStatus = getFileStatus(size, pathStr);


//            HashMap<String, String> partitionValues = getPartitionMap(partition_map, arena);

            var deletionVector= getDeletionVector(dv_info, arena);

            Optional<Expression> javaExpression = Optional.empty();
            if (!expression.equals(MemorySegment.NULL)) {
                ExpressionVisitor expressionVisitor = new ExpressionVisitor(arena, arena, expression);
                javaExpression = Optional.of(expressionVisitor.result);
            }

            context.queue.add(new RustScanFileRow(deletionVector,  pathStr, size, javaExpression));
        } catch (Throwable e) {
            System.out.println("Throw");
            throw new RuntimeException(e);
        }
    }

    private static Optional<DeletionVectorDescriptor> getDeletionVector(MemorySegment dv_info, Arena arena) throws Throwable {
        var dvVisitor = new DvVisitor();
        var descriptor = FunctionDescriptor.ofVoid(KernelStringSlice.layout(), KernelStringSlice.layout(), ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG);
        var handle = upcallHandle(DvVisitor.class, "visitDeletionVector", descriptor).bindTo(dvVisitor);
        var handler = Linker.nativeLinker().upcallStub(handle, descriptor, arena);

        var downcallDescriptor = FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
        );
        var visitDvIfPresent = Linker.nativeLinker().downcallHandle(delta_kernel_ffi_h.findOrThrow("visit_dv_if_present"), downcallDescriptor);
        visitDvIfPresent.invokeExact(dv_info, handler);
        return dvVisitor.getResult();
    }

    private static Engine getEngine() {
        Configuration hadoopConf = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConf);
        return engine;
    }

    private CloseableIterator<FilteredColumnarBatch> getDataIterator(Engine engine, FileStatus fileStatus, RoaringBitmapArray currBitmap, HashMap<String, String> map) throws IOException {
        CloseableIterator<ColumnarBatch> physicalDataIter =
                engine.getParquetHandler().readParquetFiles(
                        singletonCloseableIterator(fileStatus),
                        context.readSchema,
                        Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
                );


        var filteredColumnBatch = new CloseableIterator<FilteredColumnarBatch>() {

            @Override
            public void close() throws IOException {
                physicalDataIter.close();
            }

            @Override
            public boolean hasNext() {
                return physicalDataIter.hasNext();
            }

            @Override
            public FilteredColumnarBatch next() {
                ColumnarBatch nextDataBatch = physicalDataIter.next();

                int rowIndexOrdinal = nextDataBatch.getSchema().indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);

                // Get the selectionVector if DV is present
                ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
                Optional<ColumnVector> selectionVector = Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
                if (rowIndexOrdinal != -1) {
                    nextDataBatch = nextDataBatch.withDeletedColumnAt(rowIndexOrdinal);
                }

                // Add partition columns
                nextDataBatch =
                        PartitionUtils.withPartitionColumns(
                                engine.getExpressionHandler(),
                                nextDataBatch,
                                map,
                                context.logicalSchema);


                return new FilteredColumnarBatch(nextDataBatch, selectionVector);
            }
        };
        return filteredColumnBatch;
    }

    private RoaringBitmapArray getSelectionVector(MemorySegment dv_info, Arena arena) {
        MemorySegment row_idxs = row_indexes_from_dv(arena, dv_info, context.engine.segment(), context.globalScanState);
        if (ExternResultKernelRowIndexArray.tag(row_idxs) != 0) {
            throw new RuntimeException("Failed");
        }
        var row_idx_vec = ExternResultKernelRowIndexArray.ok(row_idxs);
        MemorySegment arr = KernelRowIndexArray.ptr(row_idx_vec);
        long len = KernelRowIndexArray.len(row_idx_vec);
        var currBitmap = new RoaringBitmapArray();
        for (int i = 0; i < len; i++) {
            var val = arr.getAtIndex(AddressLayout.JAVA_LONG, i);
            currBitmap.add(val);
        }
        free_row_indexes(row_idx_vec);
        return currBitmap;
    }

    private HashMap<String, String> getPartitionMap(MemorySegment partition_map, Arena arena) throws Throwable {
        // Get partition map
        var get_from_string_map = Linker.nativeLinker().downcallHandle(delta_kernel_ffi_h.findOrThrow("get_from_string_map"), FunctionDescriptor.of(
                delta_kernel_ffi_h.C_POINTER,
                ValueLayout.ADDRESS,
                KernelStringSlice.layout(),
                delta_kernel_ffi_h.C_POINTER
        ));
        HashMap<String, String> map = new HashMap<>();
        for (String col : context.partitionColumns) {
            var stringFn = AllocateStringFn.allocate(new Utils.AllocateStringHandler(arena), arena);
            var colSlice = KernelStringSlice.allocate(arena);
            var msg = arena.allocateFrom(col);
            KernelStringSlice.ptr(colSlice, msg);
            KernelStringSlice.len(colSlice, col.length());
            MemorySegment nullableStringRef = (MemorySegment) get_from_string_map.invoke(partition_map, colSlice, stringFn);
            if (nullableStringRef.address() == 0) continue;
            String rootStr = nullableStringRef.getString(0);
            map.put(col, rootStr);
        }
        return map;
    }

    private static FileStatus getFileStatus(long size, String pathStr) {
        FileStatus fileStatus = FileStatus.of(pathStr, size, 0);
        return fileStatus;
    }

}
