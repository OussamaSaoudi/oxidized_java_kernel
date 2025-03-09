import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static kernel.generated.delta_kernel_ffi_h.upcallHandle;

public class DvVisitor {
    private Optional<DeletionVectorDescriptor> dv;
    private MemorySegment handle;
    private MethodHandle visitDvIfPresent;
    public DvVisitor(Arena arena) {
        dv = Optional.empty();

        var descriptor = FunctionDescriptor.ofVoid(KernelStringSlice.layout(), KernelStringSlice.layout(), ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG);
        var upcallHandle = upcallHandle(DvVisitor.class, "visitDeletionVectorCallback", descriptor).bindTo(this);
        handle = Linker.nativeLinker().upcallStub(upcallHandle, descriptor, arena);


        var downcallDescriptor = FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
        );
        visitDvIfPresent = Linker.nativeLinker().downcallHandle(delta_kernel_ffi_h.findOrThrow("visit_dv_if_present"), downcallDescriptor);
    }
    /**
     *
     type DvDescriptorVisitor = extern "C" fn(
     storageType: KernelStringSlice,
     pathOrInlineDv: KernelStringSlice,
     offset: Option<&i32>,
     sizeInBytes: i32,
     cardinality: i64,
     );
     */
    public void visitDeletionVectorCallback(MemorySegment storageType, MemorySegment pathOrInlineDv, MemorySegment offset, int sizeInBytes, long cardinality) {
        String storageTypeStr = KernelStringSlice.segmentToString(storageType);
        String pathOrInlineDvStr = KernelStringSlice.segmentToString(pathOrInlineDv);
        Optional<Integer> offsetVal = Optional.empty();
        if (!offset.equals(MemorySegment.NULL)) {
            offsetVal = Optional.of(offset.reinterpret(4).getAtIndex(AddressLayout.JAVA_INT,0));
        }
        dv = Optional.of(new DeletionVectorDescriptor(storageTypeStr, pathOrInlineDvStr, offsetVal, sizeInBytes, cardinality));
    }

    public Optional<DeletionVectorDescriptor> visitDeletionVector(MemorySegment dvInfo) throws Throwable {
        dv = Optional.empty();
        visitDvIfPresent.invokeExact(dvInfo, handle);
        return dv;
    }
    Optional<DeletionVectorDescriptor> getResult() {
        return dv;
    }
}
