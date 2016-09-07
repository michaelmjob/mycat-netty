package io.mycat.netty;

import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Created by snow_young on 16/9/7.
 */
public class TestUtil {
    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

    public static void ROWOutput(List<RowDataPacket> rows) {
        logger.info("length : {}", rows.size());
        for (RowDataPacket row : rows) {
            StringBuilder builder = new StringBuilder();
            for (byte[] field : row.fieldValues) {
                logger.info("field value :  {}", field);
                builder.append(new String(field)).append(" ,");
            }
            logger.info("field value : {}", builder.toString());
        }
    }

    // 没有数据返回，就是null, 这里会报null指针异常，需要处理
    public static void OKOutput(OkPacket okPacket) {
        logger.info("affectedRows : {}", okPacket.affectedRows);
        logger.info("insertId : {}", okPacket.insertId);
        logger.info("serverStatus : {}", okPacket.serverStatus);
        logger.info("warningCount : {}", okPacket.warningCount);
        if (!Objects.isNull(okPacket.message)) {
            logger.info("message : {}", new String(okPacket.message));
        }
    }
}
