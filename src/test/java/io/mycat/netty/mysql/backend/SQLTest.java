package io.mycat.netty.mysql.backend;

import io.mycat.netty.mysql.MySQLSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by snow_young on 16/8/13.
 */
public class SQLTest {

//    @Test
    public void insert(){


    }

    @Test
    public void delete(){

    }

    @Test
    public void update(){
        // 假设这是路由的结果
        RouteResultset nodes = new RouteResultset("update mytable set t_author='mysql_proxy' where t_title='mysql_proxyed'");
        RouteResultsetNode node = new RouteResultsetNode("d0", "update mytable set t_author='mysql_proxy' where t_title='mysql_proxyed'");
        RouteResultsetNode[] nodearr = new RouteResultsetNode[1];
        nodearr[0] = node;
        nodes.setNodes(nodearr);

        // mock frontsession
//        MySQLSession frontSession = new MySQLSession();
//        frontSession.

        MysqlSessionContext context = new MysqlSessionContext(null);

        // 从node中获取session
        // should be moved to sessionContext
        ConcurrentHashMap<RouteResultsetNode, NettyBackendSession> target = new ConcurrentHashMap<>();
        for(RouteResultsetNode nodeiter : nodes.getNodes()){
            // prepare get session
//            nodeiter
        }
//        context.setTarget();


    }

    @Test
    public void select(){

    }
}
