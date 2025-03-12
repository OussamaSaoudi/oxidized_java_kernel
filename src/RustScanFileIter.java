import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.ExternResultbool;
import kernel.generated.kernel_scan_data_next$engine_visitor;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

import static kernel.generated.delta_kernel_ffi_h.*;

public class RustScanFileIter implements CloseableIterator<RustScanFileRow> {

    private final Arena arena;
    private final MemorySegment dataIter;
    public final EngineContext context;
    public boolean isDone;
    private final MemorySegment scanDataCallback;
    InvokeVisitScanData invokeVisitScanData;

    public RustScanFileIter(Arena arena, RustEngine engine, RustScan scan, String rootStr) {
        this.arena = arena;
        this.isDone = false;
        var dataIterRes = new KernelResult(kernel_scan_data_init(arena, engine.segment(), scan.segment()));
        if (dataIterRes.isErr()) {
            var err = dataIterRes.err();
            throw new RuntimeException("Failed to create scan data iterator");
        }
        dataIter = dataIterRes.ok();
        context = new EngineContext(engine, scan, rootStr);
        invokeVisitScanData = new InvokeVisitScanData(arena, context);
        scanDataCallback = kernel_scan_data_next$engine_visitor.allocate(invokeVisitScanData, arena);

        fetchBatch();
    }

    public void fetchBatch() {
        MemorySegment res= kernel_scan_data_next(arena, dataIter, MemorySegment.NULL, scanDataCallback);
        if (ExternResultbool.tag(res) != 0) {
            throw new RuntimeException("Failed to get next");
        }
        isDone = !ExternResultbool.ok(res);
    }

    @Override
    public boolean hasNext() {
        if (!context.queue.isEmpty()) {
            return true;
        }
        fetchBatch();
        return !context.queue.isEmpty();
    }

    @Override
    public RustScanFileRow next() {
        return context.queue.poll();
    }

    @Override
    public void close() throws IOException {
        isDone = true;
    }
}