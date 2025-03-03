import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

import static kernel.generated.delta_kernel_ffi_h.*;


class PredicateVisitor implements EnginePredicate.visitor.Function{
    @Override
    public long apply(MemorySegment _x0, MemorySegment _x1) {
        System.out.println("Applied predicate");
        return 0;
    }
}
public class Main {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            System.out.println("Setting path");
            KernelStringSlice path = new KernelStringSlice(arena, "/Users/oussama.saoudi/oxidized_java_kernel/delta-kernel-rs/kernel/tests/data/app-txn-checkpoint");
            System.out.println("Path: + " + path);
            System.out.println("Constructing error fn");
            System.out.println("getting engine builder");
            var builder = new RustEngineBuilder(arena, path);
            var engine = builder.build();

            System.out.println("Getting Snapshot");
            var snapshot = new RustSnapshot(arena, engine, path);


            var version = snapshot.version();
            System.out.println("Version: " + version);

            var rootStr = snapshot.tableRoot();
            System.out.println("table root: "+rootStr);

            var scan = new RustScan(arena, snapshot, engine, null);

            var dataIterRes = new KernelResult(kernel_scan_data_init(arena, engine.segment(), scan.segment()));
            if (dataIterRes.isErr()) {
                var err = dataIterRes.err();
                throw new RuntimeException("Failed to create scan data iterator");
            }
            var data_iter = dataIterRes.ok();

            EngineContext context = new EngineContext(arena, scan.segment(), rootStr);

            var visit_scan_data_callback = kernel_scan_data_next$engine_visitor.allocate(new InvokeVisitScanData(arena, context), arena);
//            // iterate scan files
//            for (;;) {
//                MemorySegment ok_res = kernel_scan_data_next(arena, data_iter, MemorySegment.NULL, visit_scan_data_callback);
//                if (ExternResultbool.tag(ok_res)!= Okbool()) {
//                    throw new RuntimeException("Failed to get next");
//                } else if (!ExternResultbool.ok(ok_res)) {
//                    System.out.println("Scan data iterator done\n");
//                    break;
//                }
//            }
            var filteredColumnBatch = new RustScanFileIter(arena, engine, scan, rootStr);

            while (filteredColumnBatch.hasNext()) {
                FilteredColumnarBatch logicalData = filteredColumnBatch.next();
                ColumnarBatch dataBatch = logicalData.getData();
//                System.out.println("Data schema: " + dataBatch.getSchema());
//                System.out.println("Got data batch: " + dataBatch.getSize());

                // Not all rows in `dataBatch` are in the selected output.
                // An optional selection vector determines whether a row with a
                // specific row index is in the final output or not.
                Optional<ColumnVector> selectionVector = logicalData.getSelectionVector();

                // access the data for the column at ordinal 0
                for (CloseableIterator<Row> it = logicalData.getRows(); it.hasNext(); ) {
                    Row row = it.next();
                    ScanRowCallback.printRow(row);
                }
//                for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
//                    // check if the row is selected or not
//                    if (!selectionVector.isPresent() || // there is no selection vector, all records are selected
//                            (!selectionVector.get().isNullAt(rowIndex) && selectionVector.get().getBoolean(rowIndex))) {
//                        // Assuming the column type is String.
//                        // If it is a different type, call the relevant function on the `ColumnVector`
//                        System.out.println(column0.getString(rowIndex));
//                    }
//                }
            }


        }
    }
}