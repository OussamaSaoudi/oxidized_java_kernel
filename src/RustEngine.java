import io.delta.kernel.engine.Engine;
import kernel.generated.KernelBoolSlice;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

public class RustEngine  {
    private final Arena arena;
    private final MemorySegment segment;

    public RustEngine(Arena arena, MemorySegment segment) {
        this.arena = arena;
        this.segment = segment;


        // Setup destructor
        segment.reinterpret(arena, engine -> {
            try {
                engine_cleanup.HANDLE.invokeExact(engine);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    public RustEngine(MemorySegment segment) {
        this(Arena.ofAuto(), segment);
    }

    public MemorySegment segment() {
        return segment;
    }


    private static class engine_cleanup {
        public static final FunctionDescriptor DESC = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("free_engine");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }
}
