package io.mycat.netty.mysql.response;

import io.mycat.netty.Session;
import io.mycat.netty.mysql.auth.PrivilegeFactory;
import io.mycat.netty.mysql.packet.*;
import io.mycat.netty.mysql.proto.Proto;
import io.mycat.netty.util.StringUtil;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * header fields[]  eof rowdata lasteof
 *
 * Created by snow_young on 16/8/13.
 */
@Data
public class ResultSetPacket {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetPacket.class);

    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;


    private int resultStauts = RESULT_STATUS_INIT;
    private ResultSetHeaderPacket header = new ResultSetHeaderPacket();
    private final List<FieldPacket> fields = new ArrayList<>();
    private final List<RowDataPacket> rows = new ArrayList<>();


    public void read(byte[] data){
        switch (resultStauts){
            case RESULT_STATUS_INIT:
                logger.info("result set packet status header");
                resultStauts = RESULT_STATUS_HEADER;
                header.read(data);
                break;
            case RESULT_STATUS_HEADER:
                switch (data[4]) {
                    case EOFPacket.FIELD_COUNT:
                        logger.info("result set packet status header field");
                        resultStauts = RESULT_STATUS_FIELD_EOF;
                        break;
                    default:
                        logger.info("result set packet status add header field");
                        FieldPacket fieldPacket = new FieldPacket();
                        fieldPacket.read(data);
                        fields.add(fieldPacket);
                        break;
                }
                break;
            case RESULT_STATUS_FIELD_EOF:
                switch(data[4]) {
                    case EOFPacket.FIELD_COUNT:
                        logger.info("result set packet status field eof");
                        resultStauts = RESULT_STATUS_INIT;
                        break;
                    default:
                        logger.info("result set packet status finished");
                        RowDataPacket row = new RowDataPacket(fields.size());
                        row.read(data);
                        rows.add(row);
                        break;
                }
                break;
        }
    }

    public boolean isFinished(){
        return resultStauts == RESULT_STATUS_INIT;
    }
}
