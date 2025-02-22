// Generated by jextract

package kernel.generated;

import java.lang.invoke.*;
import java.lang.foreign.*;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.foreign.ValueLayout.*;
import static java.lang.foreign.MemoryLayout.PathElement.*;

/**
 * {@snippet lang=c :
 * struct ExternResultKernelBoolSlice {
 *     ExternResultKernelBoolSlice_Tag tag;
 *     union {
 *         struct {
 *             struct KernelBoolSlice ok;
 *         };
 *         struct {
 *             struct EngineError *err;
 *         };
 *     };
 * }
 * }
 */
public class ExternResultKernelBoolSlice {

    ExternResultKernelBoolSlice() {
        // Should not be called directly
    }

    private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
        delta_kernel_ffi_h.C_INT.withName("tag"),
        MemoryLayout.paddingLayout(4),
        MemoryLayout.unionLayout(
            MemoryLayout.structLayout(
                KernelBoolSlice.layout().withName("ok")
            ).withName("$anon$1249:5"),
            MemoryLayout.structLayout(
                delta_kernel_ffi_h.C_POINTER.withName("err")
            ).withName("$anon$1252:5")
        ).withName("$anon$1248:3")
    ).withName("ExternResultKernelBoolSlice");

    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return $LAYOUT;
    }

    private static final OfInt tag$LAYOUT = (OfInt)$LAYOUT.select(groupElement("tag"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * ExternResultKernelBoolSlice_Tag tag
     * }
     */
    public static final OfInt tag$layout() {
        return tag$LAYOUT;
    }

    private static final long tag$OFFSET = 0;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * ExternResultKernelBoolSlice_Tag tag
     * }
     */
    public static final long tag$offset() {
        return tag$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * ExternResultKernelBoolSlice_Tag tag
     * }
     */
    public static int tag(MemorySegment struct) {
        return struct.get(tag$LAYOUT, tag$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * ExternResultKernelBoolSlice_Tag tag
     * }
     */
    public static void tag(MemorySegment struct, int fieldValue) {
        struct.set(tag$LAYOUT, tag$OFFSET, fieldValue);
    }

    private static final GroupLayout ok$LAYOUT = (GroupLayout)$LAYOUT.select(groupElement("$anon$1248:3"), groupElement("$anon$1249:5"), groupElement("ok"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * struct KernelBoolSlice ok
     * }
     */
    public static final GroupLayout ok$layout() {
        return ok$LAYOUT;
    }

    private static final long ok$OFFSET = 8;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * struct KernelBoolSlice ok
     * }
     */
    public static final long ok$offset() {
        return ok$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * struct KernelBoolSlice ok
     * }
     */
    public static MemorySegment ok(MemorySegment struct) {
        return struct.asSlice(ok$OFFSET, ok$LAYOUT.byteSize());
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * struct KernelBoolSlice ok
     * }
     */
    public static void ok(MemorySegment struct, MemorySegment fieldValue) {
        MemorySegment.copy(fieldValue, 0L, struct, ok$OFFSET, ok$LAYOUT.byteSize());
    }

    private static final AddressLayout err$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("$anon$1248:3"), groupElement("$anon$1252:5"), groupElement("err"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * struct EngineError *err
     * }
     */
    public static final AddressLayout err$layout() {
        return err$LAYOUT;
    }

    private static final long err$OFFSET = 8;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * struct EngineError *err
     * }
     */
    public static final long err$offset() {
        return err$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * struct EngineError *err
     * }
     */
    public static MemorySegment err(MemorySegment struct) {
        return struct.get(err$LAYOUT, err$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * struct EngineError *err
     * }
     */
    public static void err(MemorySegment struct, MemorySegment fieldValue) {
        struct.set(err$LAYOUT, err$OFFSET, fieldValue);
    }

    /**
     * Obtains a slice of {@code arrayParam} which selects the array element at {@code index}.
     * The returned segment has address {@code arrayParam.address() + index * layout().byteSize()}
     */
    public static MemorySegment asSlice(MemorySegment array, long index) {
        return array.asSlice(layout().byteSize() * index);
    }

    /**
     * The size (in bytes) of this struct
     */
    public static long sizeof() { return layout().byteSize(); }

    /**
     * Allocate a segment of size {@code layout().byteSize()} using {@code allocator}
     */
    public static MemorySegment allocate(SegmentAllocator allocator) {
        return allocator.allocate(layout());
    }

    /**
     * Allocate an array of size {@code elementCount} using {@code allocator}.
     * The returned segment has size {@code elementCount * layout().byteSize()}.
     */
    public static MemorySegment allocateArray(long elementCount, SegmentAllocator allocator) {
        return allocator.allocate(MemoryLayout.sequenceLayout(elementCount, layout()));
    }

    /**
     * Reinterprets {@code addr} using target {@code arena} and {@code cleanupAction} (if any).
     * The returned segment has size {@code layout().byteSize()}
     */
    public static MemorySegment reinterpret(MemorySegment addr, Arena arena, Consumer<MemorySegment> cleanup) {
        return reinterpret(addr, 1, arena, cleanup);
    }

    /**
     * Reinterprets {@code addr} using target {@code arena} and {@code cleanupAction} (if any).
     * The returned segment has size {@code elementCount * layout().byteSize()}
     */
    public static MemorySegment reinterpret(MemorySegment addr, long elementCount, Arena arena, Consumer<MemorySegment> cleanup) {
        return addr.reinterpret(layout().byteSize() * elementCount, arena, cleanup);
    }
}

