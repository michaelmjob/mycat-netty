package io.mycat.netty.mysql.handler;

import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.router.RouteResultset;
import lombok.Setter;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Created by snow_young on 16/9/2.
 */
public class SyncMysqlSessionContext extends MysqlSessionContext {
    @Setter
    private CountDownLatch blocking;

    @Setter
    private Consumer<MySQLPacket> check;

    public SyncMysqlSessionContext(MysqlFrontendSession frontSession) {
        super(frontSession);
    }


    @Override
    public void send2Client(MySQLPacket mySQLPacket){
        super.send2Client(mySQLPacket);
        blocking.countDown();
        check.accept(mySQLPacket);
    }

    public void blocking() throws InterruptedException {
        this.blocking.await();
    }

}
