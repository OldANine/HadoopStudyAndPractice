package mr.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    public OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        OrderBean o1=(OrderBean)a;
        OrderBean o2=(OrderBean)b;
        int i = o1.getOrderId().compareTo(o2.getOrderId());
        return i;
    }
}
