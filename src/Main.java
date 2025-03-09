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
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kernel.generated.*;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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

    public static void bench_rust(String tablePath) {
        try (Arena arena = Arena.ofConfined()) {
            KernelStringSlice path = new KernelStringSlice(arena, tablePath);
            var builder = new RustEngineBuilder(arena, path);
            var engine = builder.build();

            var snapshot = new RustSnapshot(arena, engine, path);


            var rootStr = snapshot.tableRoot();

            var scan = new RustScan(arena, snapshot, engine, null);

            var scanFileIter = new RustScanFileIter(arena, engine, scan, rootStr);


            while (scanFileIter.hasNext()) {
                RustScanFileRow row = scanFileIter.next();
//                System.out.println(row);

            }
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
//            iterateAndPrint(scanFileColumnarBatch);
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
        int ITER = 1;
        String path = args[0];
        System.out.println("Testing for path:" + path);
        String suite = "rust";
//        if (args.length > 1) {
//            suite = args[1];
//        }

        // Create statistics objects to collect performance data
        DescriptiveStatistics rustStats = new DescriptiveStatistics();
        DescriptiveStatistics javaStats = new DescriptiveStatistics();

        // Create a meter registry with percentile support
        if (suite.equals("both") || suite.equals("rust")) {
            for (int i = 0; i < ITER; i++) {
                var startTime = System.nanoTime();
                bench_rust(path);
                rustStats.addValue((long) ((System.nanoTime() - startTime) / 1_000_000.0));
            }
        }

        if (suite.equals("both") || suite.equals("java")) {
            for (int i = 0; i < ITER; i++) {
                var startTime = System.nanoTime();
                bench_java(path);
                javaStats.addValue((long) ((System.nanoTime() - startTime) / 1_000_000.0));
            }
        }

        // Display statistics summary
        System.out.println("Rust Statistics:");
        printStatistics(rustStats);

        System.out.println("\nJava Statistics:");
        printStatistics(javaStats);

        if (suite.equals("both")) {
            Grapher.createHistogram(rustStats, javaStats);
        }

    }
    /**
     * Prints summary statistics for the given measurements
     */
    private static void printStatistics(DescriptiveStatistics stats) {
        System.out.println("Mean: " + stats.getMean() + " ms");
        System.out.println("Min: " + stats.getMin() + " ms");
        System.out.println("Max: " + stats.getMax() + " ms");
        System.out.println("Percentiles: ");
        System.out.println("\tP50: " + stats.getPercentile(50) + " ms");
        System.out.println("\tP70: " + stats.getPercentile(50) + " ms");
        System.out.println("\tP90: " + stats.getPercentile(90) + " ms");
        System.out.println("\tP99: " + stats.getPercentile(99) + " ms");
        System.out.println("Std Dev: " + stats.getStandardDeviation() + " ms");
        System.out.println("Median: " + stats.getPercentile(50) + " ms");
    }


}