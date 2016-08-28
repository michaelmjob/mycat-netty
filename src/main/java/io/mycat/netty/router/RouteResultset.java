package io.mycat.netty.router;

import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.mysql.sqlengine.mpp.HavingCols;
import io.mycat.netty.router.parser.druid.SQLMerge;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/12.
 */
@Data
public class RouteResultset implements Serializable{
    private String statement;
//    private int sqlType;
    private RouteResultsetNode[] nodes;

    private boolean canRunSlave = false;
    private final int sqlType;
    private String primaryKey;
    private int limitStart;
    private int limitSize;
    private boolean autocommit = true;

    private SQLMerge sqlMerge;

    public RouteResultset(String statement, int sqlType){
        this.statement = statement;
        this.sqlType = sqlType;
    }

    public void setHasAggrColumn(boolean hasAggrColumn) {
        if (hasAggrColumn) {
            createSQLMergeIfNull().setHasAggrColumn(true);
        }
    }

    private boolean isFinishedRoute = false;

    public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
        if (orderByCols != null && !orderByCols.isEmpty()) {
            createSQLMergeIfNull().setOrderByCols(orderByCols);
        }
    }

    private SQLMerge createSQLMergeIfNull() {
        if (sqlMerge == null) {
            sqlMerge = new SQLMerge();
        }
        return sqlMerge;
    }

    public void setHavings(HavingCols havings) {
        if (havings != null) {
            createSQLMergeIfNull().setHavingCols(havings);
        }
    }

    public void setHavingColsName(Object[] names) {
        if (names != null && names.length > 0) {
            createSQLMergeIfNull().setHavingColsName(names);
        }
    }

    public void setGroupByCols(String[] groupByCols) {
        if (groupByCols != null && groupByCols.length > 0) {
            createSQLMergeIfNull().setGroupByCols(groupByCols);
        }
    }

    public void copyLimitToNodes() {

//        if(nodes!=null)
//        {
//            for (RouteResultsetNode node : nodes)
//            {
//                if(node.getLimitSize()==-1&&node.getLimitStart()==0)
//                {
//                    node.setLimitStart(limitStart);
//                    node.setLimitSize(limitSize);
//                }
//            }
//
//        }
    }


    public void changeNodeSqlAfterAddLimit(SchemaConfig schemaConfig, String sourceDbType, String sql, int offset, int count, boolean isNeedConvert) {
//        if (nodes != null)
//        {
//
//            Map<String, String> dataNodeDbTypeMap = schemaConfig.getDataNodeDbTypeMap();
//            Map<String, String> sqlMapCache = new HashMap<>();
//            for (RouteResultsetNode node : nodes)
//            {
//                String dbType = dataNodeDbTypeMap.get(node.getName());
//                if (sourceDbType.equalsIgnoreCase("mysql"))
//                {
//                    node.setStatement(sql);   //mysql之前已经加好limit
//                } else if (sqlMapCache.containsKey(dbType))
//                {
//                    node.setStatement(sqlMapCache.get(dbType));
//                } else if(isNeedConvert)
//                {
//                    String nativeSql = PageSQLUtil.convertLimitToNativePageSql(dbType, sql, offset, count);
//                    sqlMapCache.put(dbType, nativeSql);
//                    node.setStatement(nativeSql);
//                }  else {
//                    node.setStatement(sql);
//                }
//
//                node.setLimitStart(offset);
//                node.setLimitSize(count);
//            }
//
//        }
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        if (mergeCols != null && !mergeCols.isEmpty()) {
            createSQLMergeIfNull().setMergeCols(mergeCols);
        }

    }
}
