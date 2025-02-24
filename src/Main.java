import kernel.generated.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static kernel.generated.delta_kernel_ffi_h.*;

class AllocateErrorHandler implements AllocateErrorFn.Function {
    private final Arena arena;
    public AllocateErrorHandler(Arena arena) {
        this.arena = arena;
    }
    @Override
    public MemorySegment apply(int etype, MemorySegment msg) {
        var strLen = KernelStringSlice.len(msg);
        var strPtr = KernelStringSlice.ptr(msg);
        System.out.println("Strlen: " + strLen);
        System.out.println("e type: "+ etype);
        var jMsg = strPtr.getString(0);
        System.out.println("Error type: " + etype + ": " + jMsg);
        return arena.allocateFrom(jMsg);
    }
}

class AllocateStringHandler implements AllocateStringFn.Function {
    private final Arena arena;
    public AllocateStringHandler(Arena arena) {
        this.arena = arena;
    }
    @Override
    public MemorySegment apply(MemorySegment msg) {
        var strLen = KernelStringSlice.len(msg);
        var strPtr = KernelStringSlice.ptr(msg);
//        System.out.println("Strlen: " + strLen);
//        System.out.println("strPtr: " + strPtr);
        var jMsg = strPtr.getString(0);
        return arena.allocateFrom(jMsg);
    }
}

class PredicateVisitor implements EnginePredicate.visitor.Function{
    @Override
    public long apply(MemorySegment _x0, MemorySegment _x1) {
        System.out.println("Applied predicate");
        return 0;
    }
}
public class Main {
    static MemorySegment kernelStringSliceFromString(Arena arena, String tableName) {
        MemorySegment string = arena.allocateFrom(tableName);
        MemorySegment segment = KernelStringSlice.allocate(arena);
        KernelStringSlice.len(segment, tableName.length());
        KernelStringSlice.ptr(segment, string);
        return segment;
    }
    static MemorySegment allocateErrorFn(Arena arena) {
        return AllocateErrorFn.allocate(new AllocateErrorHandler(arena), arena);
    }
    static MemorySegment allocateStringFn(Arena arena) {
        return AllocateStringFn.allocate(new AllocateStringHandler(arena), arena);
    }
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            System.out.println("Setting path");
            MemorySegment path = kernelStringSliceFromString(arena, "/Users/oussama.saoudi/oxidized_java_kernel/delta-kernel-rs/kernel/tests/data/app-txn-checkpoint");
            System.out.println("Constructing error fn");
            MemorySegment errorFn = allocateErrorFn(arena);
            System.out.println("getting engine builder");
            MemorySegment builderRes = get_engine_builder(arena, path, errorFn);
            System.out.println("extracting tag");
            var tag = ExternResultEngineBuilder.tag(builderRes);
            System.out.println("tag: " + tag);
            if (tag == ErrEngineBuilder()) {
                // Error
                var err = ExternResultEngineBuilder.err(builderRes);
                throw new RuntimeException("got error");
            }
            // Success
            System.out.println("Succcess!");
            var builder = ExternResultEngineBuilder.ok(builderRes);
            System.out.println("Builder build");
            var sharedExternEngineRes = builder_build(arena, builder);
            tag = ExternResultHandleSharedExternEngine.tag(sharedExternEngineRes);
            if (tag == ErrHandleSharedExternEngine()) {
                var err = ExternResultEngineBuilder.err(builderRes);
                throw new RuntimeException("got error");
            }
            var engine = ExternResultHandleSharedExternEngine.ok(sharedExternEngineRes);

            System.out.println("Getting Snapshot");
            var snapshotRes = snapshot(arena, path, engine);

            tag = ExternResultHandleSharedSnapshot.tag(snapshotRes);
            System.out.println("snapshot tag: " + tag);
            if (tag== ErrHandleSharedSnapshot()) {
                var err = ExternResultHandleSharedSnapshot.err(snapshotRes);
                throw new RuntimeException("Got error");
            }

            System.out.println("Extracting snapshot ");
            var snapshot = ExternResultHandleSharedSnapshot.ok(snapshotRes);
            var version = version(snapshot);
            System.out.println("Version: " + version);

            var tableRoot = snapshot_table_root(snapshot, allocateStringFn(arena));
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