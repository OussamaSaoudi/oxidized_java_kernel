import kernel.generated.EnginePredicate;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.function.Consumer;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class FFIExpression {


    private static final GroupLayout $LAYOUT = MemoryLayout.structLayout(
            delta_kernel_ffi_h.C_POINTER.withName("predicate"),
            delta_kernel_ffi_h.C_POINTER.withName("visitor")
    ).withName("EnginePredicate");

    /**
     * The layout of this struct
     */
    public static final GroupLayout layout() {
        return $LAYOUT;
    }

    private static final AddressLayout predicate$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("predicate"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * void *predicate
     * }
     */
    public static final AddressLayout predicate$layout() {
        return predicate$LAYOUT;
    }

    private static final long predicate$OFFSET = 0;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * void *predicate
     * }
     */
    public static final long predicate$offset() {
        return predicate$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * void *predicate
     * }
     */
    public static MemorySegment predicate(MemorySegment struct) {
        return struct.get(predicate$LAYOUT, predicate$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * void *predicate
     * }
     */
    public static void predicate(MemorySegment struct, MemorySegment fieldValue) {
        struct.set(predicate$LAYOUT, predicate$OFFSET, fieldValue);
    }

    /**
     * {@snippet lang=c :
     * uintptr_t (*visitor)(void *, struct KernelExpressionVisitorState *)
     * }
     */
    public static class visitor {

        visitor() {
            // Should not be called directly
        }

        /**
         * The function pointer signature, expressed as a functional interface
         */
        public interface Function {
            long apply(MemorySegment _x0, MemorySegment _x1);
        }

        private static final FunctionDescriptor $DESC = FunctionDescriptor.of(
                delta_kernel_ffi_h.C_LONG,
                delta_kernel_ffi_h.C_POINTER,
                delta_kernel_ffi_h.C_POINTER
        );

        /**
         * The descriptor of this function pointer
         */
        public static FunctionDescriptor descriptor() {
            return $DESC;
        }

        private static final MethodHandle UP$MH = delta_kernel_ffi_h.upcallHandle(EnginePredicate.visitor.Function.class, "apply", $DESC);

        /**
         * Allocates a new upcall stub, whose implementation is defined by {@code fi}.
         * The lifetime of the returned segment is managed by {@code arena}
         */
        public static MemorySegment allocate(EnginePredicate.visitor.Function fi, Arena arena) {
            return Linker.nativeLinker().upcallStub(UP$MH.bindTo(fi), $DESC, arena);
        }

        private static final MethodHandle DOWN$MH = Linker.nativeLinker().downcallHandle($DESC);

        /**
         * Invoke the upcall stub {@code funcPtr}, with given parameters
         */
        public static long invoke(MemorySegment funcPtr,MemorySegment _x0, MemorySegment _x1) {
            try {
                return (long) DOWN$MH.invokeExact(funcPtr, _x0, _x1);
            } catch (Throwable ex$) {
                throw new AssertionError("should not reach here", ex$);
            }
        }
    }

    private static final AddressLayout visitor$LAYOUT = (AddressLayout)$LAYOUT.select(groupElement("visitor"));

    /**
     * Layout for field:
     * {@snippet lang=c :
     * uintptr_t (*visitor)(void *, struct KernelExpressionVisitorState *)
     * }
     */
    public static final AddressLayout visitor$layout() {
        return visitor$LAYOUT;
    }

    private static final long visitor$OFFSET = 8;

    /**
     * Offset for field:
     * {@snippet lang=c :
     * uintptr_t (*visitor)(void *, struct KernelExpressionVisitorState *)
     * }
     */
    public static final long visitor$offset() {
        return visitor$OFFSET;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * uintptr_t (*visitor)(void *, struct KernelExpressionVisitorState *)
     * }
     */
    public static MemorySegment visitor(MemorySegment struct) {
        return struct.get(visitor$LAYOUT, visitor$OFFSET);
    }

    /**
     * Setter for field:
     * {@snippet lang=c :
     * uintptr_t (*visitor)(void *, struct KernelExpressionVisitorState *)
     * }
     */
    public static void visitor(MemorySegment struct, MemorySegment fieldValue) {
        struct.set(visitor$LAYOUT, visitor$OFFSET, fieldValue);
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
