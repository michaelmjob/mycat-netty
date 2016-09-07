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
 * <p>
 * <p>
 * instruction :
 * normal :
 * 聚合行为
 * ok ->
 * err ->
 * resultSetResponse ->
 * abnormal :
 * 存在一个errResponse:
 * 纪录错误，全部都结束的时候，输出错误的结果,
 * TODO: host geli : 熔断限流机制
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements ResponseHandler {
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

    private boolean prepared;

    private int selectedRows;

    // should be modifield : update/insert/delete no resultsetpacket
//     select resultsetpacket
    private OkPacket ok = null;
//            = new OkPacket();
    private ResultSetPacket result = null;
//        new ResultSetPacket();

    // limit N,M
    private int limitStart;
    private int limitSize;
    private int end;

    private AtomicInteger nodeCount;
    private AtomicBoolean fieldsRtn = new AtomicBoolean(false);

    // TODO: add limit support.
    // remove autoCommit
    public MultiNodeQueryHandler(RouteResultset rrs, MysqlSessionContext sessionContext) {
        super(rrs, sessionContext);
        nodeCount = new AtomicInteger(rrs.size());
        assert !Objects.isNull(rrs.getNodes());

        this.limitSize = rrs.getLimitSize();
        this.limitStart = rrs.getLimitStart();
        this.end = rrs.getLimitStart() + rrs.getLimitSize();

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

    // 有一个错误就都是错误？
    //  先定义失败，然后等待所有的响应，后面一起响应：使用错误的结果，可是怎么回滚呢？
    //  which err
    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        // can be stoppped about other session
        lock.lock();
        if (!isFailed()) {
            isFailed.set(true);
            errorMsg = new StringBuilder();
            errorMsg.append(String.valueOf(packet.errno)).append(":").append(new String(packet.message));
            mysqlSessionContext.process();
        } else {
            errorMsg.append(";").append(String.valueOf(packet.errno)).append(":").append(new String(packet.message));
        }
        lock.unlock();
        // 多个错误的处理情况
        logger.warn("error response from {} err: {} code :{}", session.getCurrentDB(), new String(packet.message), packet.errno);
        tryErrorFinished(decrementCountBy());
    }

    // 统一的错误处理行为
    protected void tryErrorFinished(boolean allEnd) {
        if (allEnd) {
            errorFinished();
        }
    }

    // refactor : obvious code
    protected ErrorPacket createErrPkg(String errmgs) {
        logger.error("create error message for {}", errmgs);
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.packetId = ++packetId;
        } finally {
            lock.unlock();
        }
        err.errno = ErrorCode.ER_UNKNOWN_ERROR;
        err.message = StringUtil.encode(errmgs, this.mysqlSessionContext.getFrontSession().getCharset());
        return err;
    }


    // TODO: add whther need to deal with data to all responseHandler
    // TODO: add call precudure deal
    // TODO: add 全局表的支持
    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        if (isFailed.get()) {
            return;
        }

        lock.lock();
        try {
            if(Objects.isNull(ok)){
                ok = new OkPacket();
                mysqlSessionContext.process();
            }
            ok.affectedRows += packet.affectedRows;
            if (packet.insertId > 0) {
                ok.insertId = (ok.insertId == 0) ? packet.insertId : Math.min(ok.insertId, packet.insertId);
            }
        } finally {
            lock.unlock();
        }

        // ahout auto commit  implementation?
        // 判是否是最后一个包,
        // TODO: 添加autocommit 支持
        if (decrementCountBy()) {
            logger.info("prepare to send");
            if (this.mysqlSessionContext.getFrontSession().isAutocommit()) {
                // TODO: add rollback action
                mysqlSessionContext.cleanBackendInfo();
            }

            // 错误处理
            if (this.isFailed() || mysqlSessionContext.isClosed()) {
                errorFinished();
                return;
            }

            lock.lock();
            try {
                ok.packetId = ++packetId;
                ok.serverStatus = mysqlSessionContext.getFrontSession().isAutocommit() ? 2 : 1;
                mysqlSessionContext.send2Client(ok);
            } finally {
                lock.unlock();
            }
        }
    }

    // 处理失败的异常
    // 可能前端已经关闭了，或者 某一个返回异常
    // strange: how to define one of abnormal condition
    protected void errorFinished() {
//        logger.error("multi node error : {}", errorMsg);
        logger.error("multi node error : {}", errorMsg.toString());
        if (!mysqlSessionContext.isClosed()) {
            this.mysqlSessionContext.send2Client(createErrPkg(errorMsg.toString()));
        }
    }


    private boolean decrementCountBy() {
        // TODO: ADD zero callback
        lock.lock();
        try {
            return nodeCount.decrementAndGet() == 0;
        } finally {
            lock.unlock();
        }
    }


    // just flush row data
    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        if (isFailed.get()) {
            return;
        }

        lock.lock();
        try {
            if (!fieldsRtn.get()) {
                // should set packetId++?
                result = new ResultSetPacket();
                result.setHeader(resultSetPacket.getHeader());
                result.setFields(resultSetPacket.getFields());
                result.setEof(resultSetPacket.getEof());
                packetId = (byte)(resultSetPacket.getFields().size() + 2);

                // for  check
                logger.info("header packetId {}",resultSetPacket.getHeader().packetId);
                List<FieldPacket> fieldsPacket = resultSetPacket.getFields();
                for (FieldPacket fieldPacket : fieldsPacket) {
                    logger.info("field packetId {}", fieldPacket.packetId);
                }
                // header = 1
                // field = 2...++ 7
                // eof 0
                logger.info("eof packetId  {}", resultSetPacket.packetId);
                fieldsRtn.compareAndSet(false, true);
            }

            this.selectedRows++;
            for (RowDataPacket rowDataPacket : resultSetPacket.getRows()) {
                // 9 ++
                logger.info("row packetId : {}", rowDataPacket.packetId);
                rowDataPacket.packetId = ++packetId;
                logger.info("my packetId : {}", packetId);
                result.getRows().add(rowDataPacket);
//                this.mysqlSessionContext.send2Client(rowDataPacket);
            }

//            result.getRows().addAll(resultSetPacket.getRows());

        } finally {
            lock.unlock();
        }

        // check result
        // 需要研究一下 autocommit 行为和客户端的一些用法
        if (decrementCountBy()) {
//            if (this.mysqlSessionContext.getFrontSession().isAutocommit()) {
////                this.mysqlSessionContext.getFrontSession()
//            } else {
//
//            }

            EOFPacket eof = resultSetPacket.getLasteof();
            eof.packetId = this.packetId++;
            result.setLasteof(eof);
            this.mysqlSessionContext.send2Client(result);

        }
        // TODO: 不太理解查询结果派发相关的逻辑
    }
}
