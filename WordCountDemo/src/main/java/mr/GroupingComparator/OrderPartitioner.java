package mr.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


// 保证相同订单ID去往同一个reduce分区
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        int partitioner=0;
        if (orderBean.getOrderId().equals("0000001")){
            partitioner=0;
        }else if(orderBean.getOrderId().equals("0000002")){
            partitioner=1;
        }else if(orderBean.getOrderId().equals("0000003")){
            partitioner=2;
        }else partitioner=3;
        return partitioner;
        // return orderBean.getOrderId().hashCode() & Integer.MAX_VALUE % i;
    }
}
