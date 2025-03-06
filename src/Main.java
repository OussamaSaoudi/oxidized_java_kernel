import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kernel.generated.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;


class PredicateVisitor implements EnginePredicate.visitor.Function {
    @Override
    public long apply(MemorySegment _x0, MemorySegment _x1) {
        return 0;
    }
}


public class Main {

    static ArrayList<Long> list = new ArrayList<>();

    public static void iterateAndPrint(CloseableIterator<FilteredColumnarBatch> filteredColumnBatch) {

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
//                RowPrinter.printRow(row);
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

    public static CloseableIterator<FilteredColumnarBatch> bench_rust(String tablePath) {
        try (Arena arena = Arena.ofConfined()) {
            KernelStringSlice path = new KernelStringSlice(arena, tablePath);
            var builder = new RustEngineBuilder(arena, path);
            var engine = builder.build();

            var snapshot = new RustSnapshot(arena, engine, path);


            var rootStr = snapshot.tableRoot();

            var scan = new RustScan(arena, snapshot, engine, null);

            var scanFileIter = new RustScanFileIter(arena, engine, scan, rootStr);

            Configuration hadoopConf = new Configuration();
            var javaEngine = DefaultEngine.create(hadoopConf);

            return new CloseableIterator<FilteredColumnarBatch>() {
                CloseableIterator<FilteredColumnarBatch> out = getEmpty();
                @Override
                public void close() throws IOException {
                    scanFileIter.close();
                }

                @Override
                public boolean hasNext() {
                    return out.hasNext() || scanFileIter.hasNext();
                }

                @Override
                public FilteredColumnarBatch next() {
                    if ( out.hasNext()) {
                        return out.next();
                    }
                    try {
                        out = out.combine(read_data(javaEngine, scanFileIter.next(), scanFileIter.context));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return out.next();
                }
            };
        }
    }
    public static CloseableIterator<FilteredColumnarBatch> getEmpty() {
        return new CloseableIterator<>() {
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

    public static void bench_java(String path) throws IOException {
        Configuration hadoopConf = new Configuration();
        var engine = DefaultEngine.create(hadoopConf);
        var table = Table.forPath(engine, path);
        var snapshot = table.getLatestSnapshot(engine);
        var scanBuilder = snapshot.getScanBuilder(engine);
        var scan = scanBuilder.build();
        CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine);

        Row scanStateRow = scan.getScanState(engine);

        // Iterate over every scanfile
        while (fileIter.hasNext()) {
            FilteredColumnarBatch scanFileColumnarBatch = fileIter.next();
            list.add((long) scanFileColumnarBatch.hashCode());
        }


    }

    private static CloseableIterator<FilteredColumnarBatch> read_data(Engine engine, RustScanFileRow row, EngineContext context) throws IOException {
        FileStatus fileStatus = FileStatus.of(row.path, row.size, 0);
        CloseableIterator<ColumnarBatch> dataIter =
                engine.getParquetHandler().readParquetFiles(
                        singletonCloseableIterator(fileStatus),
                        context.readSchema,
                        Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
                );

        var filteredDataIter = dataIter.map(batch -> applyDeletionVector(engine, row, context, batch));

        if (row.transform.isPresent()) {
            ExpressionEvaluator handler = new DefaultExpressionEvaluator(context.readSchema, row.transform.get(), context.logicalSchema);
            return filteredDataIter.map(batch -> {
                var data = batch.getData();
                var selectionVector = batch.getSelectionVector();
                var transformed = handler.eval(data);
                var transformedBatch = new DefaultColumnarBatch(data.getSize(), context.logicalSchema, new ColumnVector[]{transformed});

                return new FilteredColumnarBatch(transformedBatch, selectionVector);
            });
        } else {
            return filteredDataIter;
        }
    }

    private static FilteredColumnarBatch applyDeletionVector(Engine engine, RustScanFileRow row, EngineContext context, ColumnarBatch nextDataBatch) {
        int rowIndexOrdinal =
                nextDataBatch.getSchema().indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);
        ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
        // Get the selectionVector if DV is present
        Optional<ColumnVector> selectionVector = Optional.empty();

        if (!row.dvInfo.isEmpty()) {
            Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
                    DeletionVectorUtils.loadNewDvAndBitmap(engine, context.tableRoot, row.dvInfo.get());
            selectionVector = Optional.of(new SelectionColumnVector(dvInfo._2, rowIndexVector));
        }
        if (rowIndexOrdinal != -1) {
            nextDataBatch = nextDataBatch.withDeletedColumnAt(rowIndexOrdinal);
        }
        return new FilteredColumnarBatch(nextDataBatch, selectionVector);
    }

    public static void main(String[] args) throws IOException {
        int ITER = 10;
        String path = args[0];
        System.out.println("Testing for path:" + path);
        String suite = "rust";
        if (args.length > 1) {
            suite = args[1];
        }

        // Create a meter registry with percentile support
        MeterRegistry registry = new SimpleMeterRegistry();

        if (suite.equals("both") || suite.equals("rust")) {
            // Create a timer for Rust benchmarks
            Timer rustTimer = Timer.builder("rust.benchmark")
                    .description("Rust benchmark execution time")
                    .publishPercentiles(0.5, 0.90, 0.95, 0.99) // Publish median, p90, p95, p99
                    .register(registry);

            for (int i = 0; i < ITER; i++) {
                rustTimer.record(() -> {
                    iterateAndPrint(bench_rust(path));
                });
            }

            // Print Rust stats
            System.out.println("Rust stats: ");
            printTimerStats(rustTimer);
        }

        System.out.println(list.size());
        if (suite.equals("both") || suite.equals("java")) {
            // Create a timer for Java benchmarks
            Timer javaTimer = Timer.builder("java.benchmark")
                    .description("Java benchmark execution time")
                    .publishPercentiles(0.5, 0.90, 0.95, 0.99) // Publish median, p90, p95, p99
                    .register(registry);

            for (int i = 0; i < ITER; i++) {
                javaTimer.record(() -> {
                    try {
                        bench_java(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Print Java stats
            System.out.println("Java stats: ");
            printTimerStats(javaTimer);
        }
        System.out.println(list.size());
    }

    private static void printTimerStats(Timer timer) {
        // Get the meter registry from the timer
        System.out.println("\tCount: " + timer.count());

        // For min, we don't have a direct method, but we can use the snapshot
        if (timer.count() > 0) {
            // Calculate mean and display it
            double meanMs = timer.mean(TimeUnit.MILLISECONDS);
            System.out.println("\tAverage: " + meanMs);

            // Get max value
            double maxMs = timer.max(TimeUnit.MILLISECONDS);
            System.out.println("\tMax: " + maxMs);

            // Get percentiles - these will only work if you've configured the timer to record them
            System.out.println("\t90th percentile: " + timer.percentile(0.9, TimeUnit.MILLISECONDS));
            System.out.println("\t95th percentile: " + timer.percentile(0.95, TimeUnit.MILLISECONDS));
            System.out.println("\t99th percentile: " + timer.percentile(0.99, TimeUnit.MILLISECONDS));
        } else {
            System.out.println("\tNo measurements recorded");
        }
    }


}