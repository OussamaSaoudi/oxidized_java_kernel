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
 * struct EngineIterator {
 *     void *data;
 *     const void *(*get_next)(void *);
 * }
 * }
 */
public class EngineIterator {

    EngineIterator() {
        // Should not be called directly
    }

    private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
        delta_kernel_ffi_h.C_POINTER.withName("data"),
        delta_kernel_ffi_h.C_POINTER.withName("get_next")
    ).withName("EngineIterator");

    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return $LAYOUT;
    }

    private static final AddressLayout data$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("data"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * void *data
     * }
     */
    public static final AddressLayout data$layout() {
        return data$LAYOUT;
    }

    private static final long data$OFFSET = 0;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * void *data
     * }
     */
    public static final long data$offset() {
        return data$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * void *data
     * }
     */
    public static MemorySegment data(MemorySegment struct) {
        return struct.get(data$LAYOUT, data$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * void *data
     * }
     */
    public static void data(MemorySegment struct, MemorySegment fieldValue) {
        struct.set(data$LAYOUT, data$OFFSET, fieldValue);
    }

    /**
     * {@snippet lang=c :
     * const void *(*get_next)(void *)
     * }
     */
    public static class get_next {

        get_next() {
            // Should not be called directly
        }

        /**
         * The function pointer signature, expressed as a functional interface
         */
        public interface Function {
            MemorySegment apply(MemorySegment _x0);
        }

        private static final FunctionDescriptor $DESC = FunctionDescriptor.of(
            delta_kernel_ffi_h.C_POINTER,
            delta_kernel_ffi_h.C_POINTER
        );

        /**
         * The descriptor of this function pointer
         */
        public static FunctionDescriptor descriptor() {
            return $DESC;
        }

        private static final MethodHandle UP$MH = delta_kernel_ffi_h.upcallHandle(get_next.Function.class, "apply", $DESC);

        /**
         * Allocates a new upcall stub, whose implementation is defined by {@code fi}.
         * The lifetime of the returned segment is managed by {@code arena}
         */
        public static MemorySegment allocate(get_next.Function fi, Arena arena) {
            return Linker.nativeLinker().upcallStub(UP$MH.bindTo(fi), $DESC, arena);
        }

        private static final MethodHandle DOWN$MH = Linker.nativeLinker().downcallHandle($DESC);

        /**
         * Invoke the upcall stub {@code funcPtr}, with given parameters
         */
        public static MemorySegment invoke(MemorySegment funcPtr,MemorySegment _x0) {
            try {
                return (MemorySegment) DOWN$MH.invokeExact(funcPtr, _x0);
            } catch (Throwable ex$) {
                throw new AssertionError("should not reach here", ex$);
            }
        }
    }

    private static final AddressLayout get_next$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("get_next"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * const void *(*get_next)(void *)
     * }
     */
    public static final AddressLayout get_next$layout() {
        return get_next$LAYOUT;
    }

    private static final long get_next$OFFSET = 8;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * const void *(*get_next)(void *)
     * }
     */
    public static final long get_next$offset() {
        return get_next$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * const void *(*get_next)(void *)
     * }
     */
    public static MemorySegment get_next(MemorySegment struct) {
        return struct.get(get_next$LAYOUT, get_next$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * const void *(*get_next)(void *)
     * }
     */
    public static void get_next(MemorySegment struct, MemorySegment fieldValue) {
        struct.set(get_next$LAYOUT, get_next$OFFSET, fieldValue);
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

