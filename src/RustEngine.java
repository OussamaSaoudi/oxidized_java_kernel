import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class RustEngine {
    private final Arena arena;
    private  final MemorySegment segment;
    public RustEngine(Arena arena, MemorySegment segment) {
        this.arena = arena;
        this.segment = segment;
    }
    public RustEngine(MemorySegment segment) {
        this(Arena.ofAuto(), segment);
    }

    public MemorySegment segment() {
        return segment;
    }
}
