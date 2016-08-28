package io.mycat.netty.router.partition;

import java.util.Map;

/**
 * Created by snow_young on 16/8/27.
 */
public class PartitionByJumpConsistentHash extends AbstractPartition implements Partition{

    private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;
    private static final long JUMP = 1L << 31;
    // If JDK >= 1.8, just use Long.parseUnsignedLong("2862933555777941757") instead.
    private static final long CONSTANT = Long.parseLong("286293355577794175", 10) * 10 + 7;

    private int totalBuckets;

    @Override
    public void init(Map<String, String> params) {

    }

    @Override
    public int caculate(String columnValue) {
        return jumpConsistentHash(columnValue.hashCode(), totalBuckets);
    }

    public static int jumpConsistentHash(final long key, final int buckets) {
        checkBuckets(buckets);
        long k = key;
        long b = -1;
        long j = 0;

        while (j < buckets) {
            b = j;
            k = k * CONSTANT + 1L;

            j = (long) ((b + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
        }
        return (int) b;
    }

    private static void checkBuckets(final int buckets) {
        if (buckets < 0) {
            throw new IllegalArgumentException("Buckets cannot be less than 0");
        }
    }

    private static double toDouble(final long n) {
        double d = n & UNSIGNED_MASK;
        if (n < 0) {
            d += 0x1.0p63;
        }
        return d;
    }

    public void setTotalBuckets(int totalBuckets) {
        this.totalBuckets = totalBuckets;
    }

    public int getTotalBuckets() {
        return totalBuckets;
    }
}