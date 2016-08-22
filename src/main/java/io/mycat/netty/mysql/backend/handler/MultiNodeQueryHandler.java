package io.mycat.netty.mysql.backend.handler;


import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.*;
import io.mycat.netty.mysql.response.ErrorCode;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by snow_young on 16/8/13.
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

    private boolean prepared;
    protected byte packetId;

    private int selectedRows;

    private MysqlSessionContext sessionContext;

    // should be modifield : update/insert/delete no resultsetpacket
    // select resultsetpacket
    private OkPacket ok = new OkPacket();
    private ErrorPacket error = new ErrorPacket();
    private ResultSetPacket result = new ResultSetPacket();

    // limit N,M
    private int limitStart;
    private int limitSize;
    private int end ;

    private AtomicInteger nodeCount;
    private AtomicBoolean fieldsRtn;

    public MultiNodeQueryHandler(int sqlType, RouteResultset rrs, boolean autocommit, MysqlSessionContext sessionContext){
        super(rrs, sessionContext);
        nodeCount = new AtomicInteger(rrs.getNodes().length);
        assert !Objects.isNull(rrs.getNodes());

        this.limitSize = rrs.getLimitSize();
        this.limitStart = rrs.getLimitStart();
        this.end  = rrs.getLimitStart() + rrs.getLimitSize();

        // do this in netty
//        if (this.limitStart < 0)
//            this.limitStart = 0;
//
//        if (rrs.getLimitSize() < 0)
//            end = Integer.MAX_VALUE;
//        if ((dataMergeSvr != null)
//                && LOGGER.isDebugEnabled()) {
//            LOGGER.debug("has data merge logic ");
//        }


    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        // can be stoppped about other session
        isFailed.compareAndSet(false , true);
        error = packet;
        // 		LOGGER.warn("error response from " + conn + " err " + errmsg + " code:"
//        + err.errno);
        tryErrorFinished(decrementCountBy());
    }

    protected void tryErrorFinished(boolean allEnd) {
        if (allEnd) {
            //
            if (errorRepsponsed.compareAndSet(false, true)) {
                this.sessionContext.getFrontSession().writeAndFlush(createErrPkg("multi node failed").getPacket());
            }
            // clear session resources,release all
//            if (session.getSource().isAutocommit()) {
//                session.closeAndClearResources(error);
//            } else {
//                session.getSource().setTxInterrupt(this.error);
//                 clear resouces
//                clearResources();
//            }
        }
    }

    // refactor : obvious code
    protected ErrorPacket createErrPkg(String errmgs) {
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.packetId = ++packetId;
        } finally {
            lock.unlock();
        }
        err.errno = ErrorCode.ER_UNKNOWN_ERROR;
        err.message = StringUtil.encode(errmgs, this.sessionContext.getFrontSession().getCharset());
        return err;
    }



    // TODO: add whther need to deal with data to all responseHandler
    // TODO: add call precudure deal
    // TODO: add 全局表的支持
    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {

        lock.lock();
        try {
            ok.affectedRows += packet.affectedRows;
            if (packet.insertId > 0) {
                ok.insertId = (ok.insertId == 0) ? packet.insertId : Math.min(ok.insertId, packet.insertId);
            }
        }finally{
            lock.unlock();
        }

        // ahout auto commit  implementation?
        // 判是否是最后一个包
        // TODO: 添加autocommit 支持
        if(decrementCountBy()){
            if(this.sessionContext.getFrontSession().isAutocommit()){
                // TODO: add rollback action
                sessionContext.releaseConnections();
            }

            if(this.isFailed() || sessionContext.isClosed()){
                tryErrorFinished();
                return;
            }

            lock.lock();
            try{
                ok.packetId = ++packetId;
                ok.serverStatus = sessionContext.getFrontSession().isAutocommit() ? 2 : 1;

                sessionContext.getFrontSession().writeAndFlush(ok.getPacket());

            }finally{
                lock.unlock();
            }

        }

    }

    // 处理失败的异常
    protected void tryErrorFinished(){
        if(!sessionContext.isClosed()){
            if(sessionContext.getFrontSession().isAutocommit()){
                sessionContext.closeAndClearResources();
            }else{
                // 非自动提交的处理
            }
        }
    }


    protected boolean decrementCountBy(){
        // TODO: ADD zero callback
        lock.lock();
        try{
            return nodeCount.decrementAndGet() == 0;
        }finally {
            lock.unlock();
        }
    }


    // just flush row data
    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        if(isFailed.get()){
            return;
        }

        lock.lock();
        try{
            if(!fieldsRtn.get()){
                ResultSetHeaderPacket resultSetHeaderPacket = resultSetPacket.getHeader();
                List<FieldPacket> fieldsPacket = resultSetPacket.getFields();
                this.mysqlSessionContext.getFrontSession().writeAndFlush(resultSetHeaderPacket.getPacket());
                for(FieldPacket fieldPacket : fieldsPacket) {
                    this.mysqlSessionContext.getFrontSession().writeAndFlush(fieldPacket.getPacket());
                }
                fieldsRtn.compareAndSet(false, true);
            }

            this.selectedRows ++;
            for(RowDataPacket rowDataPacket : resultSetPacket.getRows()){
                rowDataPacket.packetId = ++ packetId;
                this.mysqlSessionContext.getFrontSession().writeAndFlush(rowDataPacket.getPacket());
            }

        }finally {
            lock.unlock();
        }

        // check result
        if(decrementCountBy()){
            if(this.mysqlSessionContext.getFrontSession().isAutocommit()){
//                this.mysqlSessionContext.getFrontSession()
            }else{

            }

            EOFPacket eof = resultSetPacket.getLasteof();
            eof.packetId = this.packetId++;
            this.mysqlSessionContext.getFrontSession().writeAndFlush(eof.getPacket());
        }
        // TODO: 不太理解查询结果派发相关的逻辑
    }



}
