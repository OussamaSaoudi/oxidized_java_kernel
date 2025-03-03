import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import kernel.generated.ExternResultbool;
import kernel.generated.kernel_scan_data_next$engine_visitor;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

import static kernel.generated.delta_kernel_ffi_h.*;

public class RustScanFileIter implements CloseableIterator<FilteredColumnarBatch> {

    private final Arena arena;
    private final MemorySegment dataIter;
    private final EngineContext context;
    private boolean isDone;
    private final MemorySegment scanDataCallback;

    public RustScanFileIter(Arena arena, RustEngine engine, RustScan scan, String rootStr) {
        this.arena = arena;
        this.isDone = false;
        var dataIterRes = new KernelResult(kernel_scan_data_init(arena, engine.segment(), scan.segment()));
        if (dataIterRes.isErr()) {
            var err = dataIterRes.err();
            throw new RuntimeException("Failed to create scan data iterator");
        }
        dataIter = dataIterRes.ok();
        context = new EngineContext(scan.segment(), rootStr);
        scanDataCallback = kernel_scan_data_next$engine_visitor.allocate(new InvokeVisitScanData(arena, context), arena);

        fetchBatch();
    }
    private void fetchBatch() {
        MemorySegment ok_res = kernel_scan_data_next(arena, dataIter, MemorySegment.NULL, scanDataCallback);
        if (ExternResultbool.tag(ok_res) != Okbool()) {
            throw new RuntimeException("Failed to get next");
        } else if (!ExternResultbool.ok(ok_res)) {
            isDone = true;
        }
    }

    @Override
    public boolean hasNext() {
        if (context.queue.hasNext())
            return true;
        if (isDone)
            return false;
        // Fill the queue
        fetchBatch();
        return context.queue.hasNext();
    }

    @Override
    public FilteredColumnarBatch next() {
        var out = context.queue.next();

        if (!context.queue.hasNext()) {
            fetchBatch();
        }
        return out;
    }

    @Override
    public void close() throws IOException {
        context.queue.close();
        isDone = true;
    }
}