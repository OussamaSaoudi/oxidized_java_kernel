package kernel.oxidized_java;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RowPrinter {
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
