import io.delta.kernel.expressions.*;
import kernel.generated.AllocateErrorFn;
import kernel.generated.EnginePredicate;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PredicateVisitor implements EnginePredicate.visitor.Function {
    private final Expression rootExpression;
    private final Map<Expression, Long> expressionToId = new HashMap<>();

    public PredicateVisitor(Expression rootExpression) {
        this.rootExpression = rootExpression;
    }

    @Override
    public long apply(MemorySegment predicatePtr, MemorySegment statePtr) {
        // This is the entry point where the Rust code calls our visitor
        return convertExpression(rootExpression, statePtr);
    }

    // This method handles converting Java expressions to Rust expressions
    private long convertExpression(Expression expr, MemorySegment statePtr) {
        // Check if we've already converted this expression
        if (expressionToId.containsKey(expr)) {
            return expressionToId.get(expr);
        }

        try {
            // Handle different expression types
            return switch (expr) {
                case Literal literal -> convertLiteral(literal, statePtr);
                case Column column -> convertColumn(column, statePtr);
                case And and -> convertAnd(and, statePtr);
                case null, default -> {
                    assert expr != null;
                    throw new UnsupportedOperationException("Unsupported expression type: " + expr.getClass().getName());
                }
            };
        } catch (Throwable t) {
            throw new RuntimeException("Failed to convert expression: " + expr, t);
        }
    }

    private long convertLiteral(Literal literal, MemorySegment statePtr) throws Throwable {
        Object value = literal.getValue();
        long id;

        if (value instanceof String) {
            id = convertStringLiteral((String) value, statePtr);
        } else if (value instanceof Integer) {
            id = convertIntLiteral((Integer) value, statePtr);
        } else if (value instanceof Long) {
            id = convertLongLiteral((Long) value, statePtr);
        } else if (value instanceof Short) {
            id = convertShortLiteral((Short) value, statePtr);
        } else if (value instanceof Byte) {
            id = convertByteLiteral((Byte) value, statePtr);
        } else if (value instanceof Float) {
            id = convertFloatLiteral((Float) value, statePtr);
        } else if (value instanceof Double) {
            id = convertDoubleLiteral((Double) value, statePtr);
        } else if (value instanceof Boolean) {
            id = convertBoolLiteral((Boolean) value, statePtr);
        } else {
            throw new UnsupportedOperationException("Unsupported literal type: " + value.getClass().getName());
        }

        expressionToId.put(literal, id);
        return id;
    }

    private long convertIntLiteral(int value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_int"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertLongLiteral(long value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_long"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertShortLiteral(short value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_short"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertByteLiteral(byte value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_byte"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertFloatLiteral(float value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_float"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_FLOAT)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertDoubleLiteral(double value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_double"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertBoolLiteral(boolean value, MemorySegment statePtr) throws Throwable {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_bool"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_BOOLEAN)
        );

        return (long) handle.invokeExact(statePtr, value);
    }

    private long convertStringLiteral(String value, MemorySegment statePtr) throws Throwable {
        Arena arena = Arena.ofConfined();

        // Create the string slice
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);

        // Allocate memory for the string data (adding 1 for null terminator)
        MemorySegment stringData = arena.allocate(stringBytes.length + 1);

        // Copy bytes to the memory segment
        for (int i = 0; i < stringBytes.length; i++) {
            stringData.set(ValueLayout.JAVA_BYTE, i, stringBytes[i]);
        }
        // Add null terminator
        stringData.set(ValueLayout.JAVA_BYTE, stringBytes.length, (byte) 0);

        // Create the string slice struct
        MemorySegment stringSlice = KernelStringSlice.allocate(arena);
        KernelStringSlice.ptr(stringSlice, stringData);
        KernelStringSlice.len(stringSlice, stringBytes.length);

        // Create an error allocator function
        MemorySegment errorAllocator = createErrorAllocator(arena);

        // Call the Rust function
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_literal_string"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS,
                        KernelStringSlice.layout(), ValueLayout.ADDRESS)
        );

        return (long) handle.invokeExact(statePtr, stringSlice, errorAllocator);
    }

    private long convertColumn(Column column, MemorySegment statePtr) throws Throwable {
        Arena arena = Arena.ofConfined();

        // Create the string slice for column name
        String name = column.toString();
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        // Allocate memory for the string data (adding 1 for null terminator)
        MemorySegment stringData = arena.allocate(nameBytes.length + 1);

        // Copy bytes to the memory segment
        for (int i = 0; i < nameBytes.length; i++) {
            stringData.set(ValueLayout.JAVA_BYTE, i, nameBytes[i]);
        }
        // Add null terminator
        stringData.set(ValueLayout.JAVA_BYTE, nameBytes.length, (byte) 0);

        // Create the string slice struct
        MemorySegment stringSlice = KernelStringSlice.allocate(arena);
        KernelStringSlice.ptr(stringSlice, stringData);
        KernelStringSlice.len(stringSlice, nameBytes.length);

        // Create an error allocator function
        MemorySegment errorAllocator = createErrorAllocator(arena);

        // Call the Rust function
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_column"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS,
                        KernelStringSlice.layout(), ValueLayout.ADDRESS)
        );

        long id = (long) handle.invokeExact(statePtr, stringSlice, errorAllocator);
        expressionToId.put(column, id);
        return id;
    }

    // Implementation for AND expressions
    private long convertAnd(And and, MemorySegment statePtr) throws Throwable {
        List<Expression> children = and.getChildren();

//        // Creates children expressions first
//        List<Long> childIds = new ArrayList<>();
//        for (Expression child : children) {
//            long childId = convertExpression(child, statePtr);
//            childIds.add(childId);
//        }

        // We need to create an EngineIterator to pass to visit_expression_and
        // Since we don't have the exact implementation of EngineIterator.

        // This is a simplified placeholder that doesn't actually call the visit_expression_and function
        MethodHandle handle = Linker.nativeLinker().downcallHandle(
                delta_kernel_ffi_h.findOrThrow("visit_expression_and"),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );

        // Placeholder for actual implementation
        long id = -1; // This should be the result of calling visit_expression_and

        // Need to implement createEngineIterator to properly pass child IDs
        // Arena arena = Arena.ofConfined();
        // MemorySegment iterator = createEngineIterator(childIds, arena);
        // id = (long) handle.invokeExact(statePtr, iterator);

        expressionToId.put(and, id);
        return (long) handle.invokeExact(statePtr);

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


    // Helper method to use this with the existing code
    public static MemorySegment createEnginePredicate(Expression expression, Arena arena) {
        // Create the visitor
        PredicateVisitor visitor = new PredicateVisitor(expression);

        // Allocate memory for the engine predicate
        MemorySegment enginePredicate = FFIExpression.allocate(arena);

        // Create the function pointer for the visitor
        MemorySegment visitorFnPtr = EnginePredicate.visitor.allocate(visitor, arena);

        // Set the visitor function in the enginePredicate
        FFIExpression.visitor(enginePredicate, visitorFnPtr);

        // Set the predicate pointer to NULL (it doesn't seem to be used in the Rust code)
        FFIExpression.predicate(enginePredicate, MemorySegment.NULL);

        return enginePredicate;
    }
}
