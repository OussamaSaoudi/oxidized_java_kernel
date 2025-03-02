import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class RustEngine {
    private final Arena arena;
    public RustEngine(Arena arena, String path) {
        this.arena = arena;
    }
    public RustEngine(String path) {
        // This arena's lifetime will be tied to `this`
        this.arena = Arena.ofAuto();
    }

    /** Wraps a MemorySegment that represents a rust kernel engine. This handles low-level operations related to the
     *  kernel engnie.
     */
    private class RustKernelEngineSegment {
        private final MemorySegment segment;
        RustKernelEngineSegment(MemorySegment segment) {
            this.segment = segment;
        }
    }
}
