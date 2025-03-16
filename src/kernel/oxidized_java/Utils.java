package kernel.oxidized_java;

import kernel.generated.AllocateErrorFn;
import kernel.generated.AllocateStringFn;
import kernel.generated.KernelStringSlice;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class Utils {

    static MemorySegment allocateErrorFn(Arena arena) {
        return AllocateErrorFn.allocate(new Utils.AllocateErrorHandler(arena), arena);
    }
    static MemorySegment allocateStringFn(Arena arena) {
        return AllocateStringFn.allocate(new Utils.AllocateStringHandler(arena), arena);
    }
    static class AllocateErrorHandler implements AllocateErrorFn.Function {
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

    static class AllocateStringHandler implements AllocateStringFn.Function {
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
}
