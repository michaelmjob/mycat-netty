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
public class ResultSetPacket extends MySQLPacket {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetPacket.class);

    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;


    private int resultStauts = RESULT_STATUS_INIT;
    private ResultSetHeaderPacket header = new ResultSetHeaderPacket();
    private List<FieldPacket> fields = new ArrayList<>();
    private List<RowDataPacket> rows = new ArrayList<>();
    private EOFPacket eof = new EOFPacket();
    private EOFPacket lasteof = new EOFPacket();

    public void read(byte[] data){
        switch (resultStauts){
            case RESULT_STATUS_INIT:
                logger.info("header packet, {}", data);
                resultStauts = RESULT_STATUS_HEADER;
                header.read(data);
                break;
            case RESULT_STATUS_HEADER:
                switch (data[4]) {
                    case EOFPacket.FIELD_COUNT:
                        logger.info("field eof packet, {}", data);
                        eof.read(data);
                        resultStauts = RESULT_STATUS_FIELD_EOF;
                        break;
                    default:
                        logger.info("field packet, {}", data);
                        FieldPacket fieldPacket = new FieldPacket();
                        fieldPacket.read(data);
                        fields.add(fieldPacket);
                        break;
                }
                break;
            case RESULT_STATUS_FIELD_EOF:
                switch(data[4]) {
                    case EOFPacket.FIELD_COUNT:
                        logger.info("last eof packet, {}", data);
                        lasteof.read(data);
                        resultStauts = RESULT_STATUS_INIT;
                        break;
                    default:
                        logger.info("row data packet, {}", data);
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


    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size];
        int offset = 0;

        System.arraycopy(header.getPacket(), 0, packet, offset, header.calcPacketSize() + 4);
        offset += header.calcPacketSize() + 4;

        for(FieldPacket field : fields) {
            System.arraycopy(field.getPacket(), 0, packet, offset, field.calcPacketSize() + 4);
            offset += field.calcPacketSize() + 4;
        }

        System.arraycopy(eof.getPacket(), 0, packet, offset, eof.calcPacketSize() + 4);
        offset += eof.calcPacketSize() + 4;

        for(RowDataPacket row : rows){
            System.arraycopy(row.getPacket(), 0, packet, offset, row.calcPacketSize() + 4);
            offset += row.calcPacketSize() + 4;
        }

        System.arraycopy(lasteof.getPacket(), 0, packet, offset, lasteof.calcPacketSize() + 4);
        return packet;
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        size += header.calcPacketSize();
        size += 4;
        for(FieldPacket field : fields) {
            size += field.calcPacketSize();
            size += 4;
        }

        size += eof.calcPacketSize();
        size += 4;

        for(RowDataPacket packet : rows){
            size += packet.calcPacketSize();
            size += 4;
        }

        size += lasteof.calcPacketSize();
        size += 4;

        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "";
    }

}
