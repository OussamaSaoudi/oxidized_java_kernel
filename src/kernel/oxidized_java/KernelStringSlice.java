package kernel.oxidized_java;

import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class KernelStringSlice {


    /****************************************
     *   Memory Segment and Layout fields   *
     ****************************************/
    private static final GroupLayout LAYOUT = MemoryLayout.structLayout(
            delta_kernel_ffi_h.C_POINTER.withName("ptr"),
            delta_kernel_ffi_h.C_LONG.withName("len")
    ).withName("kernel.oxidized_java.KernelStringSlice");
    private static final AddressLayout ptr$LAYOUT = (AddressLayout) LAYOUT.select(groupElement("ptr"));
    private static final long ptr$OFFSET = 0;
    private static final ValueLayout.OfLong len$LAYOUT = (ValueLayout.OfLong) LAYOUT.select(groupElement("len"));
    private static final long len$OFFSET = 8;

    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return LAYOUT;
    }

    private final MemorySegment segment;
    /** The arena associated with this kernel.oxidized_java.KernelStringSlice MemorySegment */
    private final Arena arena;

    public KernelStringSlice(MemorySegment segment) {
        this.arena = null;
        this.segment = segment;
    }
    public KernelStringSlice(Arena arena, String string) {
        this.arena = arena;
        MemorySegment stringSegment = arena.allocateFrom(string);
        segment = arena.allocate(layout());
        len(string.length());
        ptr(stringSegment);
    }

    public KernelStringSlice(String string) {
        this(Arena.ofAuto(), string);
    }

    /**
     * Getter for field:
     * {@snippet lang = c:
     * const char *ptr
     *}
     */
    public MemorySegment ptr() {
        return this.segment.get(ptr$LAYOUT, ptr$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang = c:
     * const char *ptr
     *}
     */
    public void ptr(MemorySegment fieldValue) {
        segment.set(ptr$LAYOUT, ptr$OFFSET, fieldValue);
    }

    /**
     * Getter for field:
     * {@snippet lang = c:
     * uintptr_t len
     *}
     */
    public long len() {
        return segment.get(len$LAYOUT, len$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang = c:
     * uintptr_t len
     *}
     */
    public void len(long fieldValue) {
        segment.set(len$LAYOUT, len$OFFSET, fieldValue);
    }

    public MemorySegment segment() {
        return this.segment;
    }

    public static String segmentToString(MemorySegment segment) {
        var ptr = kernel.generated.KernelStringSlice.ptr(segment);
        var len = kernel.generated.KernelStringSlice.len(segment);
        return ptr.reinterpret(len + 1).getString(0);
    }

    @Override
    public String toString() {
        return segmentToString(this.segment);
    }
}
