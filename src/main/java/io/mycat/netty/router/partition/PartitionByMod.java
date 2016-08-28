package io.mycat.netty.router.partition;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/27.
 */
public class PartitionByMod extends AbstractPartition implements Partition {
    private int count;

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void init(Map<String, String> params) {
        count = Integer.parseInt( params.get("count"));
    }

    @Override
    public int caculate(String columnValue) {

        BigInteger bigNum = new BigInteger(columnValue).abs();
        return (bigNum.mod(BigInteger.valueOf(count))).intValue();
    }

    private static void hashTest() {
        PartitionByMod hash = new PartitionByMod();
        hash.setCount(11);
        hash.init(null);

        int[] bucket = new int[hash.count];

        Map<Integer, List<Integer>> hashed = new HashMap<>();

        int total = 1000_0000;//数据量
        int c = 0;
        for (int i = 100_0000; i < total + 100_0000; i++) {//假设分片键从100万开始
            c++;
            int h = hash.caculate(Integer.toString(i));
            bucket[h]++;
            List<Integer> list = hashed.get(h);
            if (list == null) {
                list = new ArrayList<>();
                hashed.put(h, list);
            }
            list.add(i);
        }
        System.out.println(c + "   " + total);
        double d = 0;
        c = 0;
        int idx = 0;
        System.out.println("index    bucket   ratio");
        for (int i : bucket) {
            d += i / (double) total;
            c += i;
            System.out.println(idx++ + "  " + i + "   " + (i / (double) total));
        }
        System.out.println(d + "  " + c);

        System.out.println("****************************************************");
        rehashTest(hashed.get(0));
    }


    // test
    private static void rehashTest(List<Integer> partition) {
        PartitionByMod hash = new PartitionByMod();
        hash.count = 110;//分片数
        hash.init(null);

        int[] bucket = new int[hash.count];

        int total = partition.size();//数据量
        int c = 0;
        for (int i : partition) {//假设分片键从100万开始
            c++;
            int h = hash.caculate(Integer.toString(i));
            bucket[h]++;
        }
        System.out.println(c + "   " + total);
        c = 0;
        int idx = 0;
        System.out.println("index    bucket   ratio");
        for (int i : bucket) {
            c += i;
            System.out.println(idx++ + "  " + i + "   " + (i / (double) total));
        }
    }

    public static void main(String[] args) {
        hashTest();
    }
}
