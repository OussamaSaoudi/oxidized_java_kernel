import io.delta.kernel.expressions.Expression;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;

import java.util.Map;
import java.util.Optional;

public class RustScanFileRow {
    Optional<DeletionVectorDescriptor> dvInfo;
//    Map<String, String> partitionMap;
    String path;
    long size;
    Optional<Expression> transform;

    public RustScanFileRow(Optional<DeletionVectorDescriptor> dvInfo,
//                           Map<String, String> partitionMap,
                           String path,
                           long size,
                           Optional<Expression> transform
    ) {
        this.dvInfo = dvInfo;
//        this.partitionMap = partitionMap;
        this.path = path;
        this.size = size;
        this.transform = transform;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\nPath: ").append(path).append("\n");
        if (dvInfo.isEmpty()) {
            builder.append("DV: null");
        } else {
            builder.append("DV: ").append(dvInfo.get().toString());
        }
        builder.append('\n');
//        builder.append("Partitions:");
//        for (String key : partitionMap.keySet()) {
//            builder.append("\n\t").append(key).append(" -> ").append(partitionMap.get(key));
//        }
        builder.append('\n');
        builder.append("Size: ").append(size).append("\n");
        if (transform.isEmpty()) {
            builder.append("Transform: null");
        } else {
            builder.append("Transform: ").append(transform.get()).append("\n}\n");
        }
        return builder.toString();
    }
}
