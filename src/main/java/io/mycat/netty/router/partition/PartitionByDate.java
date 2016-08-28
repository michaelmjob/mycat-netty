package io.mycat.netty.router.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by snow_young on 16/8/27.
 * 通过类加载器来实现
 */
public class PartitionByDate extends AbstractPartition implements Partition {
    private static final Logger logger = LoggerFactory.getLogger(PartitionByDate.class);

    private String sBeginDate;
    private String sEndDate;
    private String sPartionDay;
    private String dateFormat;

    private long beginDate;
    private long partionTime;
    private long endDate;
    private int nCount;

    private static final long oneDay = 86400000;

    @Override
    public void init(Map<String, String> params) {

        sPartionDay = params.get("PartionDay");
        sBeginDate = params.get("beginDate");
        dateFormat = params.get("dateFormat");
        sEndDate = params.get("endDate");

        try {
            partionTime = Integer.parseInt(sPartionDay) * oneDay;

            beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

            if (sEndDate != null && !sEndDate.equals("")) {
                endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                nCount = (int) ((endDate - beginDate) / partionTime) + 1;
            }
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }

    @Override
    public int caculate(String columnValue) {
        try {
            long targetTime = new SimpleDateFormat(dateFormat).parse(columnValue).getTime();
            int targetPartition = (int) ((targetTime - beginDate) / partionTime);

            if (targetTime > endDate && nCount != 0) {
                targetPartition = targetPartition % nCount;
            }
            return targetPartition;

        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);

        }
    }

    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return AbstractPartition.calculateSequenceRange(this, beginValue, endValue);
    }

    public void setsBeginDate(String sBeginDate) {
        this.sBeginDate = sBeginDate;
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getsEndDate() {
        return this.sEndDate;
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
    }


}
