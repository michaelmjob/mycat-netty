package io.mycat.netty.router.partition;

/**
 * Created by snow_young on 16/8/27.
 */
public class PartitionByLong extends AbstractPartition implements Partition{

    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;

    private static int[] toIntArray(String string) {
        String[] strs = SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }

    @Override
    public void init() {
        partitionUtil = new PartitionUtil(count, length);

    }

    @Override
    public int caculate(String columnValue) {
        long key = Long.parseLong(columnValue);
        return partitionUtil.partition(key);
    }

    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return AbstractPartition.calculateSequenceRange(this, beginValue, endValue);
    }
}
