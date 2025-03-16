package kernel.oxidized_java;

import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import kernel.generated.EngineSchemaVisitor;
import kernel.generated.KernelStringSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;

public class SchemaVisitor {
    MemorySegment schemaVisitor;
    StructType result;

    public SchemaVisitor(Arena arena, MemorySegment schema) {
        schemaVisitor = EngineSchemaVisitor.allocate(arena);
        var visitor = new VisitorHandles();


        var handle = upcallHandle(VisitorHandles.class, "make_field_list", make_field_list_descriptor()).bindTo(visitor);
        var handler = Linker.nativeLinker().upcallStub(handle, make_field_list_descriptor(), arena);
        EngineSchemaVisitor.make_field_list(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_struct", complex_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, complex_type_descriptor(), arena);
        EngineSchemaVisitor.visit_struct(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_array", complex_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, complex_type_descriptor(), arena);
        EngineSchemaVisitor.visit_array(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_map", complex_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, complex_type_descriptor(), arena);
        EngineSchemaVisitor.visit_map(schemaVisitor, handler);


        var descriptor = FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                KernelStringSlice.layout(),
                ValueLayout.JAVA_BOOLEAN,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_BYTE,
                ValueLayout.JAVA_BYTE
        );
        handle = upcallHandle(VisitorHandles.class, "visit_decimal", descriptor).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, descriptor, arena);
        EngineSchemaVisitor.visit_decimal(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_string", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_string(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_long", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_long(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_integer", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_integer(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_short", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_short(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_byte", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_byte(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_float", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_float(schemaVisitor, handler);


        handle = upcallHandle(VisitorHandles.class, "visit_double", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_double(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_boolean", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_boolean(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_binary", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_binary(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_timestamp", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_timestamp(schemaVisitor, handler);

        handle = upcallHandle(VisitorHandles.class, "visit_timestamp_ntz", simple_type_descriptor()).bindTo(visitor);
        handler = Linker.nativeLinker().upcallStub(handle, simple_type_descriptor(), arena);
        EngineSchemaVisitor.visit_timestamp_ntz(schemaVisitor, handler);

        var visit_expression_ref = Linker.nativeLinker().downcallHandle(delta_kernel_ffi_h.findOrThrow("visit_schema"), FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        ));
        try {
            long ret = (long) visit_expression_ref.invokeExact(schema, schemaVisitor);
            result = new StructType(visitor.fieldLists.get(ret));
        } catch (Throwable e) {
            System.out.println("Oof :( " + e);
        }
    }


    static FunctionDescriptor make_field_list_descriptor() {
        return FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);
    }

    static FunctionDescriptor visit_literal_descriptor(ValueLayout literalLayout) {
        return FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, literalLayout);
    }

    static FunctionDescriptor complex_type_descriptor() {
        return FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                KernelStringSlice.layout(),
                ValueLayout.JAVA_BOOLEAN,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
        );
    }

    static FunctionDescriptor simple_type_descriptor() {
        return FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                KernelStringSlice.layout(),
                ValueLayout.JAVA_BOOLEAN,
                ValueLayout.ADDRESS
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
        HashMap<Long, ArrayList<StructField>> fieldLists = new HashMap<>();
        HashMap<Integer, Expression> expressions = new HashMap<>();
        long counter = 0;

        long make_field_list(MemorySegment _data, long reserve) {
            var lstId = counter++;
            fieldLists.put(lstId, new ArrayList<>((int) reserve));
            return lstId;
        }

        //     void (*visit_struct)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *, uintptr_t);
        void visit_struct(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata, long child_list) {
            var list = fieldLists.get(child_list);
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, new StructType(list), nullable));
        }

        //     void (*visit_array)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *, uintptr_t);
        void visit_array(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata, long child_list) {
            var list = fieldLists.get(child_list).iterator();
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, new ArrayType(list.next()), nullable));
        }

        //     void (*visit_map)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *, uintptr_t);
        void visit_map(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata, long child_list) {
            var list = fieldLists.get(child_list).iterator();
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, new MapType(list.next(), list.next()), nullable));
        }

        //     void (*visit_decimal)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *, uint8_t, uint8_t);
        void visit_decimal(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata, byte precision, byte scale) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, new DecimalType(precision, scale), nullable));
        }

        //     void (*visit_string)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_string(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, StringType.STRING, nullable));
        }

        //     void (*visit_long)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_long(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, LongType.LONG, nullable));
        }

        //     void (*visit_integer)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_integer(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, IntegerType.INTEGER, nullable));
        }

        //     void (*visit_short)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_short(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, ShortType.SHORT, nullable));
        }

        //     void (*visit_byte)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_byte(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, ByteType.BYTE, nullable));
        }

        //     void (*visit_float)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_float(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, FloatType.FLOAT, nullable));
        }

        //     void (*visit_double)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_double(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, DoubleType.DOUBLE, nullable));
        }

        //     void (*visit_boolean)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_boolean(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, BooleanType.BOOLEAN, nullable));
        }

        //     void (*visit_binary)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_binary(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, BinaryType.BINARY, nullable));
        }

        //     void (*visit_date)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_date(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            // Date unsupported on this version of kernel java
//            var strPtr = kernel.oxidized_java.KernelStringSlice.ptr(name);
//            var nameStr = strPtr.getString(0);
//            fieldLists.get(sibling_list_id).add(new StructField(nameStr, DataType., nullable));
        }

        //     void (*visit_timestamp)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_timestamp(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, TimestampType.TIMESTAMP, nullable));
        }

        //     void (*visit_timestamp_ntz)(void *, uintptr_t, struct kernel.oxidized_java.KernelStringSlice, bool, const struct CStringMap *);
        void visit_timestamp_ntz(MemorySegment _data, long sibling_list_id, MemorySegment name, boolean nullable, MemorySegment metadata) {
            var strPtr = KernelStringSlice.ptr(name);
            var nameStr = strPtr.getString(0);
            fieldLists.get(sibling_list_id).add(new StructField(nameStr, TimestampNTZType.TIMESTAMP_NTZ, nullable));
        }
    }
}
