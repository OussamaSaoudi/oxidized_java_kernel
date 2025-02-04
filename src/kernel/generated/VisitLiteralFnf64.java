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
 * typedef void (*VisitLiteralFnf64)(void *, uintptr_t, double)
 * }
 */
public class VisitLiteralFnf64 {

    VisitLiteralFnf64() {
        // Should not be called directly
    }

    /**
     * The function pointer signature, expressed as a functional interface
     */
    public interface Function {
        void apply(MemorySegment data, long sibling_list_id, double value);
    }

    private static final FunctionDescriptor $DESC = FunctionDescriptor.ofVoid(
        delta_kernel_ffi_h.C_POINTER,
        delta_kernel_ffi_h.C_LONG,
        delta_kernel_ffi_h.C_DOUBLE
    );

    /**
     * The descriptor of this function pointer
     */
    public static FunctionDescriptor descriptor() {
        return $DESC;
    }

    private static final MethodHandle UP$MH = delta_kernel_ffi_h.upcallHandle(VisitLiteralFnf64.Function.class, "apply", $DESC);

    /**
     * Allocates a new upcall stub, whose implementation is defined by {@code fi}.
     * The lifetime of the returned segment is managed by {@code arena}
     */
    public static MemorySegment allocate(VisitLiteralFnf64.Function fi, Arena arena) {
        return Linker.nativeLinker().upcallStub(UP$MH.bindTo(fi), $DESC, arena);
    }

    private static final MethodHandle DOWN$MH = Linker.nativeLinker().downcallHandle($DESC);

    /**
     * Invoke the upcall stub {@code funcPtr}, with given parameters
     */
    public static void invoke(MemorySegment funcPtr,MemorySegment data, long sibling_list_id, double value) {
        try {
             DOWN$MH.invokeExact(funcPtr, data, sibling_list_id, value);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}

