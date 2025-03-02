import kernel.generated.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static kernel.generated.delta_kernel_ffi_h.*;


class PredicateVisitor implements EnginePredicate.visitor.Function{
    @Override
    public long apply(MemorySegment _x0, MemorySegment _x1) {
        System.out.println("Applied predicate");
        return 0;
    }
}
public class Main {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            System.out.println("Setting path");
            KernelStringSlice path = new KernelStringSlice(arena, "/Users/oussama.saoudi/oxidized_java_kernel/delta-kernel-rs/kernel/tests/data/app-txn-checkpoint");
            System.out.println("Constructing error fn");
            System.out.println("getting engine builder");
            var builder = new RustEngineBuilder(arena, path);
            var engine = builder.build();

            System.out.println("Getting Snapshot");
            var snapshotRes = snapshot(arena, path.segment(), engine);
            var kernelResult = new KernelResult(snapshotRes);

            if (kernelResult.isErr()) {
                var err = kernelResult.err();
                throw new RuntimeException("Got error");
            }

            var snapshot = kernelResult.ok();
            var version = version(snapshot);
            System.out.println("Version: " + version);

            var tableRoot = snapshot_table_root(snapshot, Utils.allocateStringFn(arena));
            String rootStr= tableRoot.getString(0);
            System.out.println("table root: "+rootStr);

            var predicate = EnginePredicate.allocate(arena);
            var visitor = EnginePredicate.visitor.allocate(new PredicateVisitor(), arena);
            EnginePredicate.visitor(predicate, visitor);
            var scanRes = scan(arena, snapshot, engine, predicate);
            if (ExternResultHandleSharedScan.tag(scanRes) == ErrHandleSharedScan()) {
                throw new RuntimeException("Failed to create scan");
            }
            var scan = ExternResultHandleSharedScan.ok(scanRes);

            var data_iter_res = kernel_scan_data_init(arena, engine, scan);
            if (ExternResultHandleSharedScanDataIterator.tag(data_iter_res) == ErrHandleSharedScanDataIterator()) {
                throw new RuntimeException("Failed to create scan data iterator");
            }
            var data_iter = ExternResultHandleSharedScanDataIterator.ok(data_iter_res);

            EngineContext context = new EngineContext(arena, scan, engine, rootStr);

            var visit_scan_data_callback = kernel_scan_data_next$engine_visitor.allocate(new InvokeVisitScanData(arena, context), arena);
            // iterate scan files
            for (;;) {
                MemorySegment ok_res = kernel_scan_data_next(arena, data_iter, context.segment(), visit_scan_data_callback);
                if (ExternResultbool.tag(ok_res)!= Okbool()) {
                    throw new RuntimeException("Failed to get next");
                } else if (!ExternResultbool.ok(ok_res)) {
                    System.out.println("Scan data iterator done\n");
                    break;
                }
            }


        }
    }
}