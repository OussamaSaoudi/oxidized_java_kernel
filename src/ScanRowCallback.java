import kernel.generated.CScanCallback;
import kernel.generated.KernelStringSlice;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class ScanRowCallback implements CScanCallback.Function {
    @Override
    public void apply(MemorySegment engine_context, MemorySegment path, long size, MemorySegment stats, MemorySegment dv_info, MemorySegment expression, MemorySegment partition_map) {
        var strLen = KernelStringSlice.len(path);
        var strPtr = KernelStringSlice.ptr(path);
        var jMsg = strPtr.getString(0);
        System.out.println(jMsg);
        try (Arena arena = Arena.ofConfined()) {
            var visitor = new ExpressionVisitor(arena, arena, expression);
            System.out.println("Yay!!");
        }
     }
}
