import kernel.generated.CScanCallback;
import kernel.generated.KernelStringSlice;
import kernel.generated.kernel_scan_data_next$engine_visitor;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;

import static kernel.generated.delta_kernel_ffi_h.*;

public class InvokeVisitScanData implements kernel_scan_data_next$engine_visitor.Function{

    private final Arena arena;
    private MemorySegment callback;
    private EngineContext context;
    ArrayList<Long> measurements = new ArrayList<>();
    public InvokeVisitScanData(Arena arena, EngineContext context) {
        this.arena = arena;
        this.context = context;
        callback = CScanCallback.allocate(new ScanRowCallback(context, arena), arena);
    }

    //        // void (*engine_visitor)(NullableCvoid, HandleExclusiveEngineData, struct KernelBoolSlice, const struct CTransforms *)
    @Override
    public void apply(MemorySegment context, MemorySegment engineData, MemorySegment selectionVector, MemorySegment transforms) {
//        var startTime = System.nanoTime();
        visit_scan_data(engineData, selectionVector, transforms, context, callback);
//        var time = (long) ((System.nanoTime() - startTime) / 1_000_000.0);
//        measurements.add(time);
//        System.out.println("Time to visit: " + time);

        free_bool_slice(selectionVector);
        free_engine_data(engineData);
    }
}