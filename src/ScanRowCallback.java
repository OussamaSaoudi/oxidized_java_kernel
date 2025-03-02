import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.*;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import kernel.generated.AllocateStringFn;
import kernel.generated.CScanCallback;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static kernel.generated.delta_kernel_ffi_h.string_slice_next;
import static kernel.generated.delta_kernel_ffi_h.upcallHandle;

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
//            var visitor = new ExpressionVisitor(arena, arena, expression);
            FileStatus fileStatus = FileStatus.of(pathStr, size, 0);

//            System.out.println("-----------------\nVisiting: " + pathStr + "\nRead Schema: " + context.readSchema + "\nLogical Schema: " + context.logicalSchema + "\nPartition columns: " + context.partitionColumns);

            // TODO(OUSSAMA): Get partition values
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
//                System.out.println("Got memory segment with size; " + nullableStringRef);
                if (nullableStringRef.address() == 0) continue;
                String rootStr = nullableStringRef.getString(0);
//                System.out.println("PartitionColumn: " + col + ":" + rootStr);
                map.put(col, rootStr);

            }
//            char* partition_val = get_from_string_map(partition_values, key, allocate_string);
//            if (partition_val) {
//                print_diag("  partition '%s' here: %s\n", col, partition_val);
//            } else {
//                print_diag("  no partition here\n");
//            }
//

            Configuration hadoopConf = new Configuration();
            Engine engine = DefaultEngine.create(hadoopConf);
            CloseableIterator<ColumnarBatch> physicalDataIter =
                    engine.getParquetHandler().readParquetFiles(
                            singletonCloseableIterator(fileStatus),
                            context.readSchema,
                            Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
                    );


            // TODO(OUSSAMA): This is a snippet  that is from the java kernel that seems like a good basis for an iterator of data batches
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

                    // TODO(OUSSAMA): Eventually add deletion vector support;
//                    DeletionVectorDescriptor dv =
//                            InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);

                    int rowIndexOrdinal = nextDataBatch.getSchema().indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);

                    // Get the selectionVector if DV is present
                    Optional<ColumnVector> selectionVector = Optional.empty();
//                    if (dv == null) {
//                        selectionVector = Optional.empty();
//                    } else {
//                        if (rowIndexOrdinal == -1) {
//                            throw new IllegalArgumentException(
//                                    "Row index column is not " + "present in the data read from the Parquet file.");
//                        }
//                        if (!dv.equals(currDV)) {
//                            Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
//                                    DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dv);
//                            this.currDV = dvInfo._1;
//                            this.currBitmap = dvInfo._2;
//                        }
//                        ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
//                        selectionVector = Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
//                    }
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

                    // TODO(OUSSAMA): Enable column mapping mode
//                    ColumnMapping.ColumnMappingMode columnMappingMode = ScanStateRow.getColumnMappingMode(scanState);
//                    switch (columnMappingMode) {
//                        case NAME: // fall through
//                        case ID:
//                            nextDataBatch = nextDataBatch.withNewSchema(logicalReadSchema);
//                            break;
//                        case NONE:
//                            break;
//                        default:
//                            throw new UnsupportedOperationException(
//                                    "Column mapping mode is not yet supported: " + columnMappingMode);
//                    }

                    return new FilteredColumnarBatch(nextDataBatch, selectionVector);
                }
            };
//            System.out.println("Iterating on filtered column batch");
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
                    printRow(row);
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
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
//        }
//
    }

    protected static void printRow(Row row){
        int numCols = row.getSchema().length();
        Object[] rowValues = IntStream.range(0, numCols)
                .mapToObj(colOrdinal -> getValue(row, colOrdinal))
                .toArray();

        // TODO: Need to handle the Row, Map, Array, Timestamp, Date types specially to
        // print them in the format they need. Copy this code from Spark CLI.

        System.out.printf(formatter(numCols), rowValues);
    }
    private static String formatter(int length) {
        return IntStream.range(0, length)
                .mapToObj(i -> "%20s")
                .collect(Collectors.joining("|")) + "\n";
    }

    private static String getValue(Row row, int columnOrdinal) {
        DataType dataType = row.getSchema().at(columnOrdinal).getDataType();
        if (row.isNullAt(columnOrdinal)) {
            return null;
        } else if (dataType instanceof BooleanType) {
            return Boolean.toString(row.getBoolean(columnOrdinal));
        } else if (dataType instanceof ByteType) {
            return Byte.toString(row.getByte(columnOrdinal));
        } else if (dataType instanceof ShortType) {
            return Short.toString(row.getShort(columnOrdinal));
        } else if (dataType instanceof IntegerType) {
            return Integer.toString(row.getInt(columnOrdinal));
        } else if (dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            int daysSinceEpochUTC = row.getInt(columnOrdinal);
            return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
        } else if (dataType instanceof LongType) {
            return Long.toString(row.getLong(columnOrdinal));
        } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            // Timestamps are stored internally as the number of microseconds since epoch.
            // TODO: TimestampType should use the session timezone to display values.
            long microSecsSinceEpochUTC = row.getLong(columnOrdinal);
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
                    microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
                    (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
                    ZoneOffset.UTC);
            return dateTime.toString();
        } else if (dataType instanceof FloatType) {
            return Float.toString(row.getFloat(columnOrdinal));
        } else if (dataType instanceof DoubleType) {
            return Double.toString(row.getDouble(columnOrdinal));
        } else if (dataType instanceof StringType) {
            return row.getString(columnOrdinal);
        } else if (dataType instanceof BinaryType) {
            return new String(row.getBinary(columnOrdinal));
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(columnOrdinal).toString();
        } else if (dataType instanceof StructType) {
            return "TODO: struct value";
        } else if (dataType instanceof ArrayType) {
            return "TODO: list value";
        } else if (dataType instanceof MapType) {
            return "TODO: map value";
        } else {
            throw new UnsupportedOperationException("unsupported data type: " + dataType);
        }
    }
}
