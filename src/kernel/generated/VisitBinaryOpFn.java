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
 * typedef void (*VisitBinaryOpFn)(void *, uintptr_t, uintptr_t)
 * }
 */
public class VisitBinaryOpFn {

    VisitBinaryOpFn() {
        // Should not be called directly
    }

    /**
     * The function pointer signature, expressed as a functional interface
     */
    public interface Function {
        void apply(MemorySegment data, long sibling_list_id, long child_list_id);
    }

    private static final FunctionDescriptor $DESC = FunctionDescriptor.ofVoid(
        delta_kernel_ffi_h.C_POINTER,
        delta_kernel_ffi_h.C_LONG,
        delta_kernel_ffi_h.C_LONG
    );

    /**
     * The descriptor of this function pointer
     */
    public static FunctionDescriptor descriptor() {
        return $DESC;
    }

    private static final MethodHandle UP$MH = delta_kernel_ffi_h.upcallHandle(VisitBinaryOpFn.Function.class, "apply", $DESC);

    /**
     * Allocates a new upcall stub, whose implementation is defined by {@code fi}.
     * The lifetime of the returned segment is managed by {@code arena}
     */
    public static MemorySegment allocate(VisitBinaryOpFn.Function fi, Arena arena) {
        return Linker.nativeLinker().upcallStub(UP$MH.bindTo(fi), $DESC, arena);
    }

    private static final MethodHandle DOWN$MH = Linker.nativeLinker().downcallHandle($DESC);

    /**
     * Invoke the upcall stub {@code funcPtr}, with given parameters
     */
    public static void invoke(MemorySegment funcPtr,MemorySegment data, long sibling_list_id, long child_list_id) {
        try {
             DOWN$MH.invokeExact(funcPtr, data, sibling_list_id, child_list_id);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}

