package kernel.oxidized_java;

import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class KernelResult {
    private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
            delta_kernel_ffi_h.C_INT.withName("tag"),
            MemoryLayout.paddingLayout(4),
            MemoryLayout.unionLayout(
                delta_kernel_ffi_h.C_POINTER.withName("ok"),
                delta_kernel_ffi_h.C_POINTER.withName("err")
            ).withName("result")
    );
    private static final ValueLayout.OfInt tag$LAYOUT = (ValueLayout.OfInt)$LAYOUT.select(groupElement("tag"));
    private static final long tag$OFFSET = 0;
    private static final AddressLayout err$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("result"), groupElement("err"));
    private static final long err$OFFSET = 8;
    private static final AddressLayout ok$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("result"), groupElement("ok"));
    private static final long ok$OFFSET = 8;
    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return $LAYOUT;
    }


    private final MemorySegment segment;
    public KernelResult(MemorySegment segment) {
        this.segment = segment;
    }


    /**
     * Getter for field:
     * {@snippet lang=c :
     * ExternResultEngineBuilder_Tag tag
     * }
     */
    private int tag() {
        return this.segment.get(tag$LAYOUT, tag$OFFSET);
    }

    public boolean isOk() {
        return tag() == 0;
    }

    public boolean isErr() {
        return tag() == 1;
    }

    public MemorySegment err() {
        return segment.get(err$LAYOUT, err$OFFSET);
    }

    public MemorySegment ok() {
        return segment.get(ok$LAYOUT, ok$OFFSET);
    }
}
