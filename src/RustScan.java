import kernel.generated.EnginePredicate;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static kernel.generated.delta_kernel_ffi_h.scan;

public class RustScan {
    private final MemorySegment segment;
    private final Arena arena;
    public RustScan(Arena arena, RustSnapshot snapshot, RustEngine engine, FFIExpression predicate) {

        // TODO: Replace enginePredicate with something that converts Java Kernel Expression to Rust
        var enginePredicate = EnginePredicate.allocate(arena);
        var visitor = EnginePredicate.visitor.allocate(new PredicateVisitor(), arena);
        EnginePredicate.visitor(enginePredicate, visitor);

        var scanRes = new KernelResult(scan(arena, snapshot.segment(), engine.segment(), enginePredicate));
        if (scanRes.isErr()) {
            var err = scanRes.err();
            throw new RuntimeException("Failed to create scan");
        }
        this.segment = scanRes.ok();
        this.arena = arena;
    }

    public MemorySegment segment() {
        return segment;
    }
}
