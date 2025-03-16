package kernel.oxidized_java;

import java.lang.foreign.MemorySegment;

public class KernelError {
    private  final MemorySegment segment;
    public KernelError(MemorySegment segment) {
        // TODO: Build an exception from this error segment. This will be the same as the error built in kernel.oxidized_java.Utils.AllocateError
        this.segment = segment;
    }
}
