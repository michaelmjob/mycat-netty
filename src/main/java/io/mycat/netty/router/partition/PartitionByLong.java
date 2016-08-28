package io.mycat.netty.router.partition;

import java.util.*;

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
    public void init(Map<String, String> params) {
        // 这里的设计有点奇怪 ！
        List<Integer> countList =  new ArrayList<>();
        List<Integer> lengthList = new ArrayList<>();

        int[] count = new int[]{};
        int[] length = new int[]{};
        int index = 0;
        List<String> list = Arrays.asList(params.get("count").split(","));
        for(String item : list){
            count[index++] = Integer.parseInt(item);
        }
        index = 0 ;
        list =  Arrays.asList(params.get("length").split(","));
        for(String item : list){
            length[index++] = Integer.parseInt(item);
        }
        partitionUtil = new PartitionUtil( count, length);
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
