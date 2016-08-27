package io.mycat.netty.router.parser.druid;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by snow_young on 16/8/27.
 */
public class RouteCalculateUnit {
    //
    private Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<String, Map<String, Set<ColumnRoutePair>>>();

    public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
        return tablesAndConditions;
    }

    // ??
    public void addShardingExpr(String tableName, String columnName, Object value) {

        Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(tableName);

        if (value == null) {
            // where a=null
            return;
        }

        if (tableColumnsMap == null) {
            tableColumnsMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
            tablesAndConditions.put(tableName, tableColumnsMap);
        }

        String uperColName = columnName.toUpperCase();
        Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

        //
        if (columValues == null) {
            columValues = new LinkedHashSet<ColumnRoutePair>();
            tablesAndConditions.get(tableName).put(uperColName, columValues);
        }

        // range  可选  一个 三种的区别！！！
        if (value instanceof Object[]) {
            for (Object item : (Object[]) value) {
                if(item == null) {
                    continue;
                }
                columValues.add(new ColumnRoutePair(item.toString()));
            }
        } else if (value instanceof RangeValue) {
            columValues.add(new ColumnRoutePair((RangeValue) value));
        } else {
            columValues.add(new ColumnRoutePair(value.toString()));
        }
    }

    public void clear() {
        tablesAndConditions.clear();
    }

}
