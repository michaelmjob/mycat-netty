package io.mycat.netty.router.partition;

/**
 * Created by snow_young on 16/8/27.
 */
public abstract class AbstractPartition implements Partition{


    @Override
    public int caculate(String columnValue) {
        return 0;
    }

    /**
     * 返回所有被路由到的节点的编号
     * 返回长度为0的数组表示所有节点都被路由（默认）
     * 返回null表示没有节点被路由到
     */
    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return new int[0];
    }

    /**
     * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
     * @param algorithm
     * @param beginValue
     * @param endValue
     * @return
     */
    public static int[] calculateSequenceRange(AbstractPartition algorithm, String beginValue, String endValue) {
        Integer begin = 0, end = 0;
        begin = algorithm.caculate(beginValue);
        end = algorithm.caculate(endValue);

        if(begin == null || end == null){
            return new int[0];
        }

        if (end >= begin) {
            int len = end-begin+1;
            int [] re = new int[len];

            for(int i =0;i<len;i++){
                re[i]=begin+i;
            }

            return re;
        }else{
            return null;
        }
    }
}
