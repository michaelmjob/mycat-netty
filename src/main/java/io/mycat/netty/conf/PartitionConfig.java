package io.mycat.netty.conf;

import io.mycat.netty.router.partition.AbstractPartition;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by snow_young on 16/8/27.
 */
@NoArgsConstructor
@Data
public class PartitionConfig {
    private String column;
    private String functionName;
    private AbstractPartition partition;
}
