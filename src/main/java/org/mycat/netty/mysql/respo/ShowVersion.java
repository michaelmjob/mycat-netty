
package org.mycat.netty.mysql.respo;

//import org.mycat.netty.mysql.result.SimpleResultSet;
import org.mycat.netty.mysql.MySQLServer;

import java.sql.ResultSet;
import java.sql.Types;


public final class ShowVersion {

    public static ResultSet getResultSet() {
//        SimpleResultSet result = new SimpleResultSet();
//        result.addColumn("VERSION", Types.VARCHAR, Integer.MAX_VALUE, 0);
//        result.addRow(MySQLServer.SERVER_VERSION);
//        return result;
        return null;
    }

    public static ResultSet getCommentResultSet() {
//        SimpleResultSet result = new SimpleResultSet();
//        result.addColumn("@@VERSION_COMMENT", Types.VARCHAR, Integer.MAX_VALUE, 0);
//        result.addRow("OpenDDAL MySQL Protocol Server");
//        return result;
        return null;
    }
}