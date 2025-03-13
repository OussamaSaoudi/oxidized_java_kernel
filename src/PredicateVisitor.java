import io.delta.kernel.expressions.*;
import kernel.generated.AllocateErrorFn;
import kernel.generated.EnginePredicate;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PredicateVisitor implements EnginePredicate.visitor.Function {
    private final Expression rootExpression;
    private final Map<Expression, Long> expressionToId = new HashMap<>();
    private final Arena arena;

    public PredicateVisitor(Expression rootExpression) {
        this.rootExpression = rootExpression;
        this.arena = Arena.ofConfined();
    }

    public void close() {
        arena.close();
    }

    @Override
    public long apply(MemorySegment predicatePtr, MemorySegment statePtr) {
        return convertExpression(rootExpression, statePtr);
    }

    private long convertExpression(Expression expr, MemorySegment statePtr) {
        // Return cached result if already processed
        if (expressionToId.containsKey(expr)) {
            return expressionToId.get(expr);
        }

        try {
            long id = switch (expr) {
                case Literal literal -> convertLiteral(literal, statePtr);
                case Column column -> convertColumn(column, statePtr);
//                case And and -> convertAnd(and, statePtr);
                case Predicate predicate -> {
                    if ("=".equals(predicate.getName())) {
                        yield convertEq(predicate, statePtr);
                    } else {
                        throw new UnsupportedOperationException("Unsupported predicate: " + predicate.getName());
                    }
                }
                default -> throw new UnsupportedOperationException("Unsupported expression: " + expr.getClass().getName());
            };

            expressionToId.put(expr, id);
            return id;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to convert expression: " + expr, t);
        }
    }

    private long convertEq(Predicate predicate, MemorySegment statePtr) throws Throwable {
        List<Expression> children = predicate.getChildren();
        if (children.size() != 2) {
            throw new IllegalArgumentException("EQ predicate must have exactly 2 children, found: " + children.size());
        }

        long leftId = convertExpression(children.get(0), statePtr);
        long rightId = convertExpression(children.get(1), statePtr);

        return delta_kernel_ffi_h.visit_expression_eq(statePtr, leftId, rightId);
    }

    private long convertLiteral(Literal literal, MemorySegment statePtr) throws Throwable {
        Object value = literal.getValue();

        return switch (value) {
            case String s -> convertStringLiteral(s, statePtr);
            case Integer i -> delta_kernel_ffi_h.visit_expression_literal_int(statePtr, i);
            case Long l -> delta_kernel_ffi_h.visit_expression_literal_long(statePtr, l);
            case Short s -> delta_kernel_ffi_h.visit_expression_literal_short(statePtr, s);
            case Byte b -> delta_kernel_ffi_h.visit_expression_literal_byte(statePtr, b);
            case Float f -> delta_kernel_ffi_h.visit_expression_literal_float(statePtr, f);
            case Double d -> delta_kernel_ffi_h.visit_expression_literal_double(statePtr, d);
            case Boolean b -> delta_kernel_ffi_h.visit_expression_literal_bool(statePtr, b);
            default -> throw new UnsupportedOperationException("Unsupported literal type: " +
                    (value != null ? value.getClass().getName() : "null"));
        };
    }

    private long convertStringLiteral(String value, MemorySegment statePtr) throws Throwable {
        // Create the string slice
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment stringData = createNullTerminatedString(stringBytes);

        // Create the string slice struct
        MemorySegment stringSlice = KernelStringSlice.allocate(this.arena);
        KernelStringSlice.ptr(stringSlice, stringData);
        KernelStringSlice.len(stringSlice, stringBytes.length);

        // Call Rust and process result
        MemorySegment result = delta_kernel_ffi_h.visit_expression_literal_string(
                this.arena, statePtr, stringSlice, createErrorAllocator(this.arena));

        return extractResultValue(result);
    }

    private long convertColumn(Column column, MemorySegment statePtr) throws Throwable {
        // Extract the actual column name from the Column object
        String actualName = column.getNames()[0];

        // Create the string slice
        byte[] nameBytes = actualName.getBytes(StandardCharsets.UTF_8);
        MemorySegment stringData = createNullTerminatedString(nameBytes);

        // Create the string slice struct
        MemorySegment stringSlice = KernelStringSlice.allocate(this.arena);
        KernelStringSlice.ptr(stringSlice, stringData);
        KernelStringSlice.len(stringSlice, nameBytes.length);

        // Call Rust and process result
        MemorySegment result = delta_kernel_ffi_h.visit_expression_column(
                this.arena, statePtr, stringSlice, createErrorAllocator(this.arena));

        long id = extractResultValue(result);

        // Avoid ID 0 which seems to cause issues
        if (id == 0) {
            id = 1000;
        }

        return id;
    }

//    private long convertAnd(And and, MemorySegment statePtr) throws Throwable {
//        List<Expression> children = and.getChildren();
//        if (children.isEmpty()) {
//            throw new IllegalArgumentException("AND predicate must have at least one child");
//        }
//
//        if (children.size() == 1) {
//            return convertExpression(children.get(0), statePtr);
//        }
//
//        // Simple approach: chain AND operations
//        long result = convertExpression(children.get(0), statePtr);
//        for (int i = 1; i < children.size(); i++) {
//            long childId = convertExpression(children.get(i), statePtr);
//            result = delta_kernel_ffi_h.visit_expression_and(statePtr, result);
//        }
//
//        return result;
//    }

    // Helper methods

    private MemorySegment createNullTerminatedString(byte[] bytes) {
        MemorySegment stringData = this.arena.allocate(bytes.length + 1);

        for (int i = 0; i < bytes.length; i++) {
            stringData.set(ValueLayout.JAVA_BYTE, i, bytes[i]);
        }
        stringData.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);

        return stringData;
    }

    // Helper method to create an error allocator function
    private MemorySegment createErrorAllocator(Arena arena) {
        // Create an error allocator function
        AllocateErrorFn.Function errorFunction = (message, len) -> {
            System.err.println("Error allocating:");
            return MemorySegment.NULL;
        };

        return AllocateErrorFn.allocate(errorFunction, arena);
    }

    private long extractResultValue(MemorySegment result) {
        int tag = result.get(ValueLayout.JAVA_INT, 0);
        long value = result.get(ValueLayout.JAVA_LONG, 8);

        if (tag == delta_kernel_ffi_h.Okusize()) {
            return value;
        } else {
            return -1; // Error case
        }
    }

    public static MemorySegment createEnginePredicate(Expression expression, Arena arena) {
        PredicateVisitor visitor = new PredicateVisitor(expression);

        MemorySegment enginePredicate = FFIExpression.allocate(arena);
        MemorySegment visitorFnPtr = EnginePredicate.visitor.allocate(visitor, arena);

        FFIExpression.visitor(enginePredicate, visitorFnPtr);
        FFIExpression.predicate(enginePredicate, MemorySegment.NULL);

        return enginePredicate;
    }
}