import io.delta.kernel.expressions.*;
import kernel.generated.AllocateErrorFn;
import kernel.generated.EngineExpressionVisitor;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;

import static kernel.generated.delta_kernel_ffi_h.visit_expression;

public class ExpressionVisitor {
    MemorySegment expressionVisitor;
    public ExpressionVisitor(SegmentAllocator allocator, Arena arena, MemorySegment expression){
        expressionVisitor = EngineExpressionVisitor.allocate(allocator);
        var visitor = new VisitorHandles();
        var handle = upcallHandle(VisitorHandles.class, "make_field_list", make_field_list_descriptor()).bindTo(visitor);


        var handler =  Linker.nativeLinker().upcallStub(handle, make_field_list_descriptor(), arena);
        EngineExpressionVisitor.make_field_list(expressionVisitor, handler);


        var ret = visit_expression(expression, expressionVisitor);
        System.out.println("Successfully visited");
    }

    static FunctionDescriptor make_field_list_descriptor() {
        return FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);
    }
    static FunctionDescriptor literal_descriptor(MemoryLayout value) {
        return FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
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
        HashMap<Integer, ArrayList<Integer>> fieldId;
        HashMap<Integer, Expression> expressions;
//        /// An opaque engine state pointer
//        pub data: *mut c_void,
//        /// Creates a new expression list, optionally reserving capacity up front
        long make_field_list(MemorySegment _data, long reserve) {
            System.out.println("Make field list");
            return 0;
        }
//        /// Visit a 32bit `integer` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_int: VisitLiteralFn<i32>,
        long visit_literal_int(MemorySegment _data, long sibling_list_id, int value) {
            System.out.println("visit literal int");
            return 0;
        }
//        /// Visit a 64bit `long`  belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_long: VisitLiteralFn<i64>,

        long visit_literal_long(MemorySegment _data, long sibling_list_id, long value) {
            System.out.println("visit literal long");
            return 0;
        }
//        /// Visit a 16bit `short` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_short: VisitLiteralFn<i16>,
        long visit_literal_short(MemorySegment _data, long sibling_list_id, short value) {
            System.out.println("visit literal short");
            return 0;
        }
//        /// Visit an 8bit `byte` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_byte: VisitLiteralFn<i8>,
        long visit_literal_byte(MemorySegment _data, long sibling_list_id, byte value) {
            System.out.println("visit literal short");
            return 0;
        }
//        /// Visit a 32bit `float` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_float: VisitLiteralFn<f32>,

        long visit_literal_float(MemorySegment _data, long sibling_list_id, float value) {
            System.out.println("visit literal float");
            return 0;
        }
//        /// Visit a 64bit `double` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_double: VisitLiteralFn<f64>,
        long visit_literal_double(MemorySegment _data, long sibling_list_id, double value) {
            System.out.println("visit literal double");
            return 0;
        }
//        /// Visit a `string` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_string: VisitLiteralFn<KernelStringSlice>,

        long visit_literal_double(MemorySegment _data, long sibling_list_id, MemorySegment kernelString) {
            System.out.println("visit literal string");
            return 0;
        }
//        /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
//        pub visit_literal_bool: VisitLiteralFn<bool>,
        long visit_literal_bool(MemorySegment _data, long sibling_list_id, boolean value) {
            System.out.println("visit literal string");
            return 0;
        }
//        /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
//        /// The timestamp is microsecond precision and adjusted to UTC.
//        pub visit_literal_timestamp: VisitLiteralFn<i64>,
//        /// Visit a 64bit timestamp belonging to the list identified by `sibling_list_id`.
//        /// The timestamp is microsecond precision with no timezone.
//        pub visit_literal_timestamp_ntz: VisitLiteralFn<i64>,
//        /// Visit a 32bit integer `date` representing days since UNIX epoch 1970-01-01.  The `date` belongs
//        /// to the list identified by `sibling_list_id`.
//        pub visit_literal_date: VisitLiteralFn<i32>,
//        /// Visit binary data at the `buffer` with length `len` belonging to the list identified by
//        /// `sibling_list_id`.
//        pub visit_literal_binary:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, buffer: *const u8, len: usize),
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
//        /// Visit a struct literal belonging to the list identified by `sibling_list_id`.
//        /// The field names of the struct are in a list identified by `child_field_list_id`.
//        /// The values of the struct are in a list identified by `child_value_list_id`.
//        pub visit_literal_struct: extern "C" fn(
//                data: *mut c_void,
//                sibling_list_id: usize,
//                child_field_list_id: usize,
//                child_value_list_id: usize,
//                ),
        long visit_literal_struct(MemorySegment _data, long sibling_list_id, long child_field_list, long child_value_list) {
            System.out.println("visit literal struct");
            return 0;
        }

//        /// Visit an array literal belonging to the list identified by `sibling_list_id`.
//        /// The values of the array are in a list identified by `child_list_id`.
//        pub visit_literal_array:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),

        long visit_literal_array(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit literal array");
            return 0;
        }
//        /// Visits a null value belonging to the list identified by `sibling_list_id.
//        pub visit_literal_null: extern "C" fn(data: *mut c_void, sibling_list_id: usize),
        long visit_literal_null(MemorySegment _data, long sibling_list_id) {
            System.out.println("visit literal null");
            return 0;
        }
//        /// Visits an `and` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the array are in a list identified by `child_list_id`
//        pub visit_and: VisitVariadicFn,
        long visit_and(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit and");
            return 0;
        }
//        /// Visits an `or` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the array are in a list identified by `child_list_id`
//        pub visit_or: VisitVariadicFn,

        long visit_or(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit or");
            return 0;
        }
//        /// Visits a `not` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expression will be in a _one_ item list identified by `child_list_id`
//        pub visit_not: VisitUnaryFn,

        long visit_not(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit not");
            return 0;
        }
//        /// Visits a `is_null` expression belonging to the list identified by `sibling_list_id`.
//        /// The sub-expression will be in a _one_ item list identified by `child_list_id`
//        pub visit_is_null: VisitUnaryFn,

        long visit_is_null(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit is_null");
            return 0;
        }
//        /// Visits the `LessThan` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_lt: VisitBinaryOpFn,

        long visit_lt(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit lt");
            return 0;
        }
//        /// Visits the `LessThanOrEqual` binary operator belonging to the list identified by `sibling_list_id`.
//        /// The operands will be in a _two_ item list identified by `child_list_id`
//        pub visit_le: VisitBinaryOpFn,

        long visit_le(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit le");
            return 0;
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
long visit_column(MemorySegment _data, long sibling_list_id, MemorySegment name) {
    System.out.println("visit column");
    return 0;
}
//        /// Visits a `StructExpression` belonging to the list identified by `sibling_list_id`.
//        /// The sub-expressions of the `StructExpression` are in a list identified by `child_list_id`
//        pub visit_struct_expr:
//        extern "C" fn(data: *mut c_void, sibling_list_id: usize, child_list_id: usize),
        long visit_struct_expr(MemorySegment _data, long sibling_list_id, long child_field_list) {
            System.out.println("visit struct expr");
            return 0;
        }
//
    }
}
