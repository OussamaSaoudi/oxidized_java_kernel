import io.delta.kernel.internal.actions.DeletionVectorDescriptor;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

public class DvVisitor {
    private Optional<DeletionVectorDescriptor> dv;
    public DvVisitor() {
        dv = Optional.empty();
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
    public void visitDeletionVector(MemorySegment storageType, MemorySegment pathOrInlineDv, MemorySegment offset, int sizeInBytes, long cardinality) {
        try (Arena arena = Arena.ofConfined()) {
            KernelStringSlice storageTypeStr = new KernelStringSlice(arena, storageType);
            KernelStringSlice pathOrInlineDvStr = new KernelStringSlice(arena, pathOrInlineDv);
            Optional<Integer> offsetVal = Optional.empty();
            if (!offset.equals(MemorySegment.NULL)) {
                offsetVal = Optional.of(offset.reinterpret(4).getAtIndex(AddressLayout.JAVA_INT,0));
            }
            dv = Optional.of(new DeletionVectorDescriptor(storageTypeStr.toString(), pathOrInlineDvStr.toString(), offsetVal, sizeInBytes, cardinality));
        }
    }
    Optional<DeletionVectorDescriptor> getResult() {
        return dv;
    }
}
