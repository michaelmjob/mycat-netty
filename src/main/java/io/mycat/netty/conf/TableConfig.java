package io.mycat.netty.conf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * according to the config :
 *  <table>
 *      <partition class="org.mycat.netty.partition.PartitionByMonth">
 *          <property name="dateFormat">yyyy-MM-dd</property>
 *          <property name="sBeginDate">2015-01-01</property>
 *      </partition>
 *      <datasource>
 *          <node datanode="d0" database="db0"/>
 *          <node datanode="d1" database="db1"/>
 *      </datasource>
 *  </table>
 *
 * Created by snow_young on 16/8/7.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableConfig {
    private String name;
    // that is extra!
    private String partitionColumn;
    // private Partition partition;
    private List<NodeConfig> datasource;

    // add temp
    private String primaryKey;
    //
    private PartitionConfig rule;

    public Collection<String> getAllNode(){
        List<String> nodes = new ArrayList<>();
        datasource.forEach(item -> {
            nodes.add(item.datanode);
        });
        return nodes;
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class NodeConfig {
        private String datanode;
        private String database;
    }
}
