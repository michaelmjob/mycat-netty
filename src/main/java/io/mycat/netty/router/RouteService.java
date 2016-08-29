package io.mycat.netty.router;

import io.mycat.netty.Session;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.parser.ServerParse;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;

/**
 * Created by snow_young on 16/8/12.
 */
@NoArgsConstructor
public class RouteService {
    private static final Logger logger = LoggerFactory.getLogger(RouteService.class);

    public static RouteResultset route(int sqlType, String stmt, MysqlSessionContext mysqlSessionContext) throws SQLNonTransientException {

        RouteResultset rrs = null;


        stmt = stmt.trim();
        rrs = RouteStrategyFactory.getRouteStrategy().route(sqlType, stmt, mysqlSessionContext);
//        rrs = RouteStrategyFactory.getRouteStrategy().route(sysconf, schema, sqlType, stmt,
//                charset, sc);
        return rrs;

////        后期添加hintsql的支持
//		/*!mycat: sql = select name from aa */
//        /*!mycat: schema = test */
////      boolean isMatchOldHint = stmt.startsWith(OLD_MYCAT_HINT);
////      boolean isMatchNewHint = stmt.startsWith(NEW_MYCAT_HINT);
////		if (isMatchOldHint || isMatchNewHint ) {
//
//        int hintLength = RouteService.isHintSql(stmt);
//        if(hintLength != -1){
//            int endPos = stmt.indexOf("*/");
//            if (endPos > 0) {
//                // 用!mycat:内部的语句来做路由分析
////				int hintLength = isMatchOldHint ? OLD_MYCAT_HINT.length() : NEW_MYCAT_HINT.length();
//                String hint = stmt.substring(hintLength, endPos).trim();
//
//                int firstSplitPos = hint.indexOf(HINT_SPLIT);
//                if(firstSplitPos > 0 ){
//                    Map hintMap=    parseHint(hint);
//                    String hintType = (String) hintMap.get(MYCAT_HINT_TYPE);
//                    String hintSql = (String) hintMap.get(hintType);
//                    if( hintSql.length() == 0 ) {
//                        LOGGER.warn("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/ or /*mycat:type=value*/: "+stmt);
//                        throw new SQLSyntaxErrorException("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/ or /*mycat:type=value*/: "+stmt);
//                    }
//                    String realSQL = stmt.substring(endPos + "*/".length()).trim();
//
//                    HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
//                    if( hintHandler != null ) {
//
//                        if ( hintHandler instanceof  HintSQLHandler) {
//                            /**
//                             * 修复 注解SQL的 sqlType 与 实际SQL的 sqlType 不一致问题， 如： hint=SELECT，real=INSERT
//                             * fixed by zhuam
//                             */
//                            int hintSqlType = ServerParse.parse( hintSql ) & 0xff;
//                            rrs = hintHandler.route(sysconf, schema, sqlType, realSQL, charset, sc, tableId2DataNodeCache, hintSql,hintSqlType,hintMap);
//
//                        } else {
//                            rrs = hintHandler.route(sysconf, schema, sqlType, realSQL, charset, sc, tableId2DataNodeCache, hintSql,sqlType,hintMap);
//                        }
//
//                    }else{
//                        LOGGER.warn("TODO , support hint sql type : " + hintType);
//                    }
//
//                }else{//fixed by runfriends@126.com
//                    LOGGER.warn("comment in sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/ or /*mycat:type=value*/: "+stmt);
//                    throw new SQLSyntaxErrorException("comment in sql must meet :/*!mcat:type=value*/ or /*#mycat:type=value*/ or /*mycat:type=value*/: "+stmt);
//                }
//            }
//        } else {
//            stmt = stmt.trim();
//            rrs = RouteStrategyFactory.getRouteStrategy().route(sysconf, schema, sqlType, stmt,
//                    charset, sc, tableId2DataNodeCache);
//        }

    }

}
