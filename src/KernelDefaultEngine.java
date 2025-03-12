import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class KernelDefaultEngine {
    private final MemorySegment segment;
    private final Arena arena;

    public KernelDefaultEngine(MemorySegment segment, Arena arena) {
        this.segment = segment;
        this.arena = arena;
        // TODO
    }
}
