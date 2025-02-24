import kernel.generated.CScanCallback;
import kernel.generated.KernelStringSlice;
import kernel.generated.kernel_scan_data_next$engine_visitor;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static kernel.generated.delta_kernel_ffi_h.visit_scan_data;

public class InvokeVisitScanData implements kernel_scan_data_next$engine_visitor.Function{

    private final Arena arena;
    private MemorySegment callback;
    private EngineContext context;
    public InvokeVisitScanData(Arena arena, EngineContext context) {
        this.arena = arena;
        this.context = context;
        callback = CScanCallback.allocate(new ScanRowCallback(context), arena);
    }

    //        // void (*engine_visitor)(NullableCvoid, HandleExclusiveEngineData, struct KernelBoolSlice, const struct CTransforms *)
    @Override
    public void apply(MemorySegment context, MemorySegment engineData, MemorySegment selectionVector, MemorySegment transforms) {
//        print_diag("\nScan iterator found some data to read\n  Of this data, here is "
//                "a selection vector\n");
//        print_selection_vector("    ", &selection_vec);
//        // Ask kernel to iterate each individual file and call us back with extracted metadata
//        print_diag("Asking kernel to call us back for each scan row (file to read)\n");
        visit_scan_data(engineData, selectionVector, transforms, context, callback);
//        free_bool_slice(selection_vec);
//        free_engine_data(engine_data);
    }
}

//public class InvokeVisitScanData {
//
//    final static Linker linker = Linker.nativeLinker();
//
//    class VisitScanData{
//        // void (*engine_visitor)(NullableCvoid, HandleExclusiveEngineData, struct KernelBoolSlice, const struct CTransforms *)
//        static void visit_scan_data(MemorySegment engineContext, MemorySegment engineData, MemorySegment selection_vector, MemorySegment c_transform) {
//            // todo
//        }
//
//        MethodHandle visitor_handle() throws NoSuchMethodException, IllegalAccessException {
//
//            MethodHandle comparHandle = MethodHandles.lookup()
//                    .findStatic(VisitScanData.class,
//                            "visit_scan_data",
//                            MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
//        }
//        FunctionDescriptor descriptor() {
//            // Create a Java description of a C function implemented by a Java method
//             return FunctionDescriptor.of(
//                    ValueLayout.JAVA_INT,
//                    ValueLayout.ADDRESS.withTargetLayout(ValueLayout.JAVA_INT),
//                    ValueLayout.ADDRESS.withTargetLayout(ValueLayout.JAVA_INT));
//
//
//        }
//        MemorySegment func() {
//            var handle = visitor_handle();
//            // Create function pointer for qsortCompare
//            MemorySegment compareFunc = linker.upcallStub(handle,
//                    descriptor(),
//                    Arena.ofAuto());
//        }
//    }
//}