package org.mycat.netty.mysql.respo;

//import org.mycat.netty.mysql.result.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.Types;

public class TxResultSet {

    public static ResultSet getReadonlyResultSet(boolean readOnly) {
//        SimpleResultSet result = new SimpleResultSet();
//        result.addColumn("@@SESSION.TX_READ_ONLY", Types.BOOLEAN, Integer.MAX_VALUE, 0);
//        result.addRow(readOnly);
//        return result;
        return null;
    }

    public static ResultSet getIsolationResultSet(int isolation) {
//        SimpleResultSet result = new SimpleResultSet();
//        result.addColumn("@@SESSION.TX_ISOLATION", Types.INTEGER, Integer.MAX_VALUE, 0);
//        result.addRow(isolation);
//        return result;
        return null;
    }

}
