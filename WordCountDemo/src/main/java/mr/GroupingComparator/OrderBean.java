package mr.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    // 比较 同笔订单按照金额进行降序排列
    @Override
    public int compareTo(OrderBean o) {
        // 先比较订单号，如果是同一笔订单，再进行价格比对
        int result=this.orderId.compareTo(o.orderId);
        if(result==0){
            result=-this.price.compareTo(o.price);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId=in.readUTF();
        this.price=in.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + '\t' + price;
    }
}
