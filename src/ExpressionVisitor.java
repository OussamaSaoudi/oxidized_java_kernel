import io.delta.kernel.expressions.*;
import io.delta.kernel.types.DataType;
import kernel.generated.AllocateErrorFn;
import kernel.generated.EngineExpressionVisitor;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;
import org.apache.commons.pool.ObjectPool;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static kernel.generated.delta_kernel_ffi_h.visit_expression;

public class ExpressionVisitor {
    MemorySegment expressionVisitor;
    Expression result;
    MethodHandle visit_expression_ref;
    VisitorHandles visitor = new VisitorHandles();

    public ExpressionVisitor(Arena arena){
        expressionVisitor = EngineExpressionVisitor.allocate(arena);


        var handle = upcallHandle(VisitorHandles.class, "make_field_list", make_field_list_descriptor()).bindTo(visitor);
        var handler =  Linker.nativeLinker().upcallStub(handle, make_field_list_descriptor(), arena);
        EngineExpressionVisitor.make_field_list(expressionVisitor, handler);

        var descriptor = visit_literal_descriptor(ValueLayout.JAVA_INT);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_int", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_int(expressionVisitor, handler);

        descriptor = visit_literal_descriptor(ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_long", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_long(expressionVisitor, handler);

        descriptor = visit_literal_descriptor(ValueLayout.JAVA_SHORT);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_short", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_short(expressionVisitor, handler);


        descriptor = visit_literal_descriptor(ValueLayout.JAVA_BYTE);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_byte", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_byte(expressionVisitor, handler);


        descriptor = visit_literal_descriptor(ValueLayout.JAVA_FLOAT);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_float", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_float(expressionVisitor, handler);

        descriptor = visit_literal_descriptor(ValueLayout.JAVA_DOUBLE);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_double", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_double(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, KernelStringSlice.layout());
        handle = upcallHandle(VisitorHandles.class, "visit_literal_string", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_string(expressionVisitor, handler);


        descriptor = visit_literal_descriptor(ValueLayout.JAVA_BOOLEAN);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_bool", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_bool(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_struct", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_bool(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_array", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_array(expressionVisitor, handler);

        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_literal_null", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_literal_null(expressionVisitor, handler);

        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_and", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_and(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_or", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_or(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_not", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_not(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_is_null", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_is_null(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_lt", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_lt(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_le", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_le(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,KernelStringSlice.layout());
        handle = upcallHandle(VisitorHandles.class, "visit_column", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_column(expressionVisitor, handler);


        descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        handle = upcallHandle(VisitorHandles.class, "visit_struct_expr", descriptor).bindTo(visitor);
        handler =  Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineExpressionVisitor.visit_struct_expr(expressionVisitor, handler);



        visit_expression_ref = Linker.nativeLinker().downcallHandle(delta_kernel_ffi_h.findOrThrow("visit_expression_ref"), FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            delta_kernel_ffi_h.C_POINTER,
            delta_kernel_ffi_h.C_POINTER
        ));
    }

    public Expression visitExpression(MemorySegment expression) throws Throwable {
        visitor.clear();
        long retIdx = (long) visit_expression_ref.invokeExact(expression, expressionVisitor);
        Expression out = visitor.expressionLists.get(retIdx).getFirst();
        visitor.clear();
        return out;
    }


    static FunctionDescriptor make_field_list_descriptor() {
        return FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);
    }
    static FunctionDescriptor visit_literal_descriptor(ValueLayout literalLayout) {
        return FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, literalLayout);
    }
    static FunctionDescriptor literal_descriptor(MemoryLayout value) {
        return FunctionDescriptor.of(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                value
        );
    }
    static FunctionDescriptor variadic_descriptor() {
        return FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
        );
    }

    static MethodHandle upcallHandle(Class<?> fi, String name, FunctionDescriptor fdesc) {
        try {
            return MethodHandles.lookup().findVirtual(fi, name, fdesc.toMethodType());
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    class VisitorHandles {
        HashMap<Long, ArrayList<Expression>> expressionLists= new HashMap<>();
        HashMap<Integer, Expression> expressions = new HashMap<>();
        long counter = 0;
        public void clear() {
            expressionLists.clear();
            counter = 0;
            expressions.clear();
        }
//        /// An opaque engine state pointer
//        pub data: *mut c_void,
//        /// Creates a new expression list, optionally reserving capacity up front
        long make_field_list(MemorySegment _data, long reserve) {
            var lstId = counter++;
            expressionLists.put(lstId, new ArrayList<>((int) reserve));
            return lstId;
        }
//        /// Visit a 32bit `integer` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_int: VisitLiteralFn<i32>,
        void visit_literal_int(MemorySegment _data, long sibling_list_id, int value) {
            expressionLists.get(sibling_list_id).add(Literal.ofInt(value));
        }
//        /// Visit a 64bit `long`  belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_long: VisitLiteralFn<i64>,

        void visit_literal_long(MemorySegment _data, long sibling_list_id, long value) {
            expressionLists.get(sibling_list_id).add(Literal.ofLong(value));
        }
//        /// Visit a 16bit `short` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_short: VisitLiteralFn<i16>,
        void visit_literal_short(MemorySegment _data, long sibling_list_id, short value) {
            expressionLists.get(sibling_list_id).add(Literal.ofShort(value));
        }
//        /// Visit an 8bit `byte` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_byte: VisitLiteralFn<i8>,
        void visit_literal_byte(MemorySegment _data, long sibling_list_id, byte value) {
            expressionLists.get(sibling_list_id).add(Literal.ofByte(value));
        }
//        /// Visit a 32bit `float` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_float: VisitLiteralFn<f32>,

        void visit_literal_float(MemorySegment _data, long sibling_list_id, float value) {
            expressionLists.get(sibling_list_id).add(Literal.ofFloat(value));
        }
//        /// Visit a 64bit `double` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_double: VisitLiteralFn<f64>,
        void visit_literal_double(MemorySegment _data, long sibling_list_id, double value) {
            expressionLists.get(sibling_list_id).add(Literal.ofDouble(value));
        }
//        /// Visit a `string` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_string: VisitLiteralFn<KernelStringSlice>,

        void visit_literal_string(MemorySegment _data, long sibling_list_id, MemorySegment kernelString) {
            var strPtr = KernelStringSlice.ptr(kernelString);
            var str = strPtr.getString(0);
            expressionLists.get(sibling_list_id).add(Literal.ofString(str));
        }
//        /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_bool: VisitLiteralFn<bool>,
        void visit_literal_bool(MemorySegment _data, long sibling_list_id, boolean value) {
            expressionLists.get(sibling_list_id).add(Literal.ofBoolean(value));
        }
//        /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
//        /// The timestamp is microsecond precision and adjusted to UTC.
//        pub visit_literal_timestamp: VisitLiteralFn<i64>,

        void visit_literal_timestamp(MemorySegment _data, long sibling_list_id, long value) {
            expressionLists.get(sibling_list_id).add(Literal.ofTimestamp(value));
        }
//        /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
//        /// The timestamp is microsecond precision with no timezone.
//        pub visit_literal_timestamp_ntz: VisitLiteralFn<i64>,

        void visit_literal_timestamp_ntz(MemorySegment _data, long sibling_list_id, long value) {
            expressionLists.get(sibling_list_id).add(Literal.ofTimestampNtz(value));
        }
//        /// Visit a 32bit integer `date` representing days since UNIX epoch 1970-01-01.  The `date` belongs
//        /// to the list identified by `sibling_list_id`.
//        pub visit_literal_date: VisitLiteralFn<i32>,
//        /// Visit binary data at the `buffer` with length `len` belonging to the list identified by
//        /// `sibling_list_id`.

        void visit_literal_date(MemorySegment _data, long sibling_list_id, int value) {
            expressionLists.get(sibling_list_id).add(Literal.ofDate(value));
        }
//        pub visit_literal_binary:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, buffer: *const u8, len: usize),

        void visit_literal_binary(MemorySegment _data, long sibling_list_id, MemorySegment buffer, long len) {
            var javaBuffer = new byte[(int) len];
            for (int i = 0; i< len; i++) {
                javaBuffer[i] = buffer.get(ValueLayout.JAVA_BYTE, i);
            }
            expressionLists.get(sibling_list_id).add(Literal.ofBinary(javaBuffer));
        }
//        /// Visit a 128bit `decimal` value with the given precision and scale. The 128bit integer
//        /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
//        /// bits in `value_ls`. The `decimal` belongs to the list identified by `sibling_list_id`.
//        pub visit_literal_decimal: extern "C" fn(
//                data: *mut c_void,
//                sibling_list_id: usize,
//                value_ms: u64,
//                value_ls: u64,
//                precision: u8,
//                scale: u8,
//                ),

        void visit_literal_decimal(MemorySegment _data, long sibling_list_id, long value_ms, long value_ls, byte precision, byte scale) {
            var value = BigInteger.valueOf(value_ms).shiftLeft(64).add(BigInteger.valueOf(value_ls));
            var value2 = new BigDecimal(value);
            expressionLists.get(sibling_list_id).add(Literal.ofDecimal(value2, precision, scale));
        }
//        /// Visit a struct literal belonging to the list identified by `sibling_list_id`.
//        /// The field names of the struct are in a list identified by `child_field_list_id`.
//        /// The values of the struct are in a list identified by `child_value_list_id`.
//        pub visit_literal_struct: extern "C" fn(
//                data: *mut c_void,
//                sibling_list_id: usize,
//                child_field_list_id: usize,
//                child_value_list_id: usize,
//                ),
        void visit_literal_struct(MemorySegment _data, long sibling_list_id, long child_field_list, long child_value_list) {
            // TODO
        }

//        /// Visit an array literal belonging to the list identified by `sibling_list_id`.
//        /// The values of the array are in a list identified by `child_list_id`.
//        pub visit_literal_array:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),

        void visit_literal_array(MemorySegment _data, long sibling_list_id, long child_field_list) {
            expressionLists.get(sibling_list_id).add(new ScalarExpression("Array",expressionLists.get(child_field_list)));
        }
//        /// Visits a null value belonging to the list identified by `sibling_list_id.
//        pub visit_literal_null: extern "C" fn(data: *mut c_void, sibling_list_id: usize),
        void visit_literal_null(MemorySegment _data, long sibling_list_id) {
//            expressionLists.get(sibling_list_id).add(Literal.ofNull());
        }
//        /// Visits an `and` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the array are in a list identified by `child_list_id`
//        pub visit_and: VisitVariadicFn,
        void visit_and(MemorySegment _data, long sibling_list_id, long child_field_list) {
            List<Expression> lst = expressionLists.get(child_field_list);
            Predicate base = AlwaysTrue.ALWAYS_TRUE;
            for (Expression elem : lst) {
                base = new And(base, (Predicate) elem);
            }
            expressionLists.get(sibling_list_id).add(base);
        }
//        /// Visits an `or` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the array are in a list identified by `child_list_id`
//        pub visit_or: VisitVariadicFn,

        void visit_or(MemorySegment _data, long sibling_list_id, long child_field_list) {
            List<Expression> lst = expressionLists.get(child_field_list);
            Predicate base = AlwaysFalse.ALWAYS_FALSE;
            for (Expression elem : lst) {
                base = new Or(base, (Predicate) elem);
            }
            expressionLists.get(sibling_list_id).add(base);
        }
//        /// Visits a `not` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expression will be in a _one_ item list identified by `child_list_id`
//        pub visit_not: VisitUnaryFn,

        void visit_not(MemorySegment _data, long sibling_list_id, long child_field_list) {
            List<Expression> lst = expressionLists.get(child_field_list);
            Predicate predicate = new Predicate("not", lst.getFirst());
            expressionLists.get(sibling_list_id).add(predicate);
        }
//        /// Visits a `is_null` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expression will be in a _one_ item list identified by `child_list_id`
//        pub visit_is_null: VisitUnaryFn,

        void visit_is_null(MemorySegment _data, long sibling_list_id, long child_field_list) {
            // Could not figure out an IS NULL
//            List<Expression> lst = expressionLists.get(child_field_list);
//            Predicate predicate = new Predicate("is", lst.getFirst());
//            expressionLists.get(sibling_list_id).add(predicate);
        }
//        /// Visits the `LessThan` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_lt: VisitBinaryOpFn,

        void visit_lt(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit lt");
        }
//        /// Visits the `LessThanOrEqual` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_le: VisitBinaryOpFn,

        void visit_le(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit le");
        }
//        /// Visits the `GreaterThan` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_gt: VisitBinaryOpFn,
//        /// Visits the `GreaterThanOrEqual` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_ge: VisitBinaryOpFn,
//        /// Visits the `Equal` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_eq: VisitBinaryOpFn,
//        /// Visits the `NotEqual` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_ne: VisitBinaryOpFn,
//        /// Visits the `Distinct` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_distinct: VisitBinaryOpFn,
//        /// Visits the `In` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_in: VisitBinaryOpFn,
//        /// Visits the `NotIn` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_not_in: VisitBinaryOpFn,
//        /// Visits the `Add` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_add: VisitBinaryOpFn,
//        /// Visits the `Minus` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_minus: VisitBinaryOpFn,
//        /// Visits the `Multiply` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_multiply: VisitBinaryOpFn,
//        /// Visits the `Divide` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_divide: VisitBinaryOpFn,
//        /// Visits the `column` belonging to the list identified by `sibling_list_id`.
//        pub visit_column:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
        void visit_column(MemorySegment _data, long sibling_list_id, MemorySegment name) {
            var strPtr = KernelStringSlice.ptr(name);
            var jMsg = strPtr.getString(0);
            expressionLists.get(sibling_list_id).add(new Column(jMsg));
        }
//        /// Visits a `StructExpression` belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the `StructExpression` are in a list identified by `child_list_id`
//        pub visit_struct_expr:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
        void visit_struct_expr(MemorySegment _data, long sibling_list_id, long child_field_list) {
            expressionLists.get(sibling_list_id).add(new ScalarExpression("Struct", expressionLists.get(child_field_list)));
        }
//
    }
}
