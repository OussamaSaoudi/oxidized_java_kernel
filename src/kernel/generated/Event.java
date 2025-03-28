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
 * struct Event {
 *     struct kernel.oxidized_java.KernelStringSlice message;
 *     enum Level level;
 *     struct kernel.oxidized_java.KernelStringSlice target;
 *     uint32_t line;
 *     struct kernel.oxidized_java.KernelStringSlice file;
 * }
 * }
 */
public class Event {

    Event() {
        // Should not be called directly
    }

    private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
        KernelStringSlice.layout().withName("message"),
        delta_kernel_ffi_h.C_INT.withName("level"),
        MemoryLayout.paddingLayout(4),
        KernelStringSlice.layout().withName("target"),
        delta_kernel_ffi_h.C_INT.withName("line"),
        MemoryLayout.paddingLayout(4),
        KernelStringSlice.layout().withName("file")
    ).withName("Event");

    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return $LAYOUT;
    }

    private static final GroupLayout message$LAYOUT = (GroupLayout)$LAYOUT.select(groupElement("message"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice message
     * }
     */
    public static final GroupLayout message$layout() {
        return message$LAYOUT;
    }

    private static final long message$OFFSET = 0;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice message
     * }
     */
    public static final long message$offset() {
        return message$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice message
     * }
     */
    public static MemorySegment message(MemorySegment struct) {
        return struct.asSlice(message$OFFSET, message$LAYOUT.byteSize());
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice message
     * }
     */
    public static void message(MemorySegment struct, MemorySegment fieldValue) {
        MemorySegment.copy(fieldValue, 0L, struct, message$OFFSET, message$LAYOUT.byteSize());
    }

    private static final OfInt level$LAYOUT = (OfInt)$LAYOUT.select(groupElement("level"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * enum Level level
     * }
     */
    public static final OfInt level$layout() {
        return level$LAYOUT;
    }

    private static final long level$OFFSET = 16;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * enum Level level
     * }
     */
    public static final long level$offset() {
        return level$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * enum Level level
     * }
     */
    public static int level(MemorySegment struct) {
        return struct.get(level$LAYOUT, level$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * enum Level level
     * }
     */
    public static void level(MemorySegment struct, int fieldValue) {
        struct.set(level$LAYOUT, level$OFFSET, fieldValue);
    }

    private static final GroupLayout target$LAYOUT = (GroupLayout)$LAYOUT.select(groupElement("target"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice target
     * }
     */
    public static final GroupLayout target$layout() {
        return target$LAYOUT;
    }

    private static final long target$OFFSET = 24;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice target
     * }
     */
    public static final long target$offset() {
        return target$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice target
     * }
     */
    public static MemorySegment target(MemorySegment struct) {
        return struct.asSlice(target$OFFSET, target$LAYOUT.byteSize());
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice target
     * }
     */
    public static void target(MemorySegment struct, MemorySegment fieldValue) {
        MemorySegment.copy(fieldValue, 0L, struct, target$OFFSET, target$LAYOUT.byteSize());
    }

    private static final OfInt line$LAYOUT = (OfInt)$LAYOUT.select(groupElement("line"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * uint32_t line
     * }
     */
    public static final OfInt line$layout() {
        return line$LAYOUT;
    }

    private static final long line$OFFSET = 40;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * uint32_t line
     * }
     */
    public static final long line$offset() {
        return line$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * uint32_t line
     * }
     */
    public static int line(MemorySegment struct) {
        return struct.get(line$LAYOUT, line$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * uint32_t line
     * }
     */
    public static void line(MemorySegment struct, int fieldValue) {
        struct.set(line$LAYOUT, line$OFFSET, fieldValue);
    }

    private static final GroupLayout file$LAYOUT = (GroupLayout)$LAYOUT.select(groupElement("file"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice file
     * }
     */
    public static final GroupLayout file$layout() {
        return file$LAYOUT;
    }

    private static final long file$OFFSET = 48;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice file
     * }
     */
    public static final long file$offset() {
        return file$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice file
     * }
     */
    public static MemorySegment file(MemorySegment struct) {
        return struct.asSlice(file$OFFSET, file$LAYOUT.byteSize());
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * struct kernel.oxidized_java.KernelStringSlice file
     * }
     */
    public static void file(MemorySegment struct, MemorySegment fieldValue) {
        MemorySegment.copy(fieldValue, 0L, struct, file$OFFSET, file$LAYOUT.byteSize());
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

