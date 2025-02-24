import kernel.generated.KernelStringSlice;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Queue;

public class StringSliceIter {
    ArrayList<String> list;

    public StringSliceIter() {
        this.list = new ArrayList<>();
    }

    public void apply(MemorySegment _data, MemorySegment slice) {
        var strPtr = KernelStringSlice.ptr(slice);
        var partitionColumn = strPtr.getString(0);
        list.add(partitionColumn);
    }
}
