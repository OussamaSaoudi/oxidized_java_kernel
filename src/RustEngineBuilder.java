import kernel.generated.ExternResultEngineBuilder;
import kernel.generated.ExternResultHandleSharedExternEngine;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static kernel.generated.delta_kernel_ffi_h.*;
import static kernel.generated.delta_kernel_ffi_h.ErrHandleSharedExternEngine;

public class RustEngineBuilder {
    private final MemorySegment segment;
    private final Arena arena;

    public RustEngineBuilder(Arena arena, KernelStringSlice path) {
        this.arena = arena;
        try (Arena builderArena = Arena.ofConfined()) {
            // The error allocation function is confined. This deallocates it once this scope ends
            MemorySegment errorFn = Utils.allocateErrorFn(builderArena);

            MemorySegment builderRes = get_engine_builder(arena, path.segment(), errorFn);

            var kernelResult = new KernelResult(builderRes);
            if (kernelResult.isErr()) {
                // Error
                var err = kernelResult.err();
                throw new RuntimeException("got error");
            }

            this.segment = kernelResult.ok();
        }
    }
    public RustEngineBuilder(KernelStringSlice path) {
        this(Arena.ofAuto(), path);
    }

    public MemorySegment build() {
        var buildRes = builder_build(arena, segment);
        var kernelResult = new KernelResult(buildRes);
        if (kernelResult.isErr()) {
            var err = kernelResult.err();
            throw new RuntimeException("got error");
        }
        return kernelResult.ok();
    }
}
