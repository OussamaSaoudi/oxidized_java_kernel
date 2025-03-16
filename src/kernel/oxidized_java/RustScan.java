package kernel.oxidized_java;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.EnginePredicate;
import kernel.generated.delta_kernel_ffi_h;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static kernel.generated.delta_kernel_ffi_h.scan;

public class RustScan {
    private final MemorySegment segment;
    private final Arena arena;
    private final RustSnapshot snapshot;
    public RustScan(Arena arena, RustSnapshot snapshot, RustEngine engine, FFIExpression predicate) {
        this.snapshot = snapshot;
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

        // Setup destructor
        segment.reinterpret(arena, scan -> {
            try {
                scan_cleanup.HANDLE.invokeExact(scan);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }


    public CloseableIterator<RustScanFileRow> getScanFiles(Engine engine) {
        if (!(engine instanceof  RustEngine)) {
            throw new RuntimeException("kernel.oxidized_java.RustScan only supports Rust Engine");
        }
        return new RustScanFileIter(arena, (RustEngine) engine, this, snapshot.tableRoot(), snapshot);
    }

    public Optional<Predicate> getRemainingFilter() {
        // TODO
        return Optional.empty();
    }

    public Row getScanState(Engine engine) {
        // TODO
        return null;
    }

    private static class scan_cleanup {
        public static final FunctionDescriptor DESC = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

        public static final MemorySegment ADDR = delta_kernel_ffi_h.findOrThrow("free_scan");

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(ADDR, DESC);
    }

    public MemorySegment segment() {
        return segment;
    }
}
