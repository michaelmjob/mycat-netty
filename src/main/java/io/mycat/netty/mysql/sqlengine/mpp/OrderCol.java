package io.mycat.netty.mysql.sqlengine.mpp;

/**
 * Created by snow_young on 16/8/28.
 */
public class OrderCol {

    public final int orderType;
    public final ColMeta colMeta;

    public static final int COL_ORDER_TYPE_ASC = 0; // ASC
    public static final int COL_ORDER_TYPE_DESC = 1; // DESC

    public OrderCol(ColMeta colMeta, int orderType) {
        super();
        this.colMeta = colMeta;
        this.orderType = orderType;
    }

    public int getOrderType() {
        return orderType;
    }

    public ColMeta getColMeta() {
        return colMeta;
    }

}