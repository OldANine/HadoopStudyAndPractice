package mr.partition;

/*
自定义分区
通过返回固定的值，设置返回值  == hashcode

 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionCustom extends Partitioner<Text,PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int i) {
        int partition=0;

        String str = text.toString();
        if(str.equals("kar")){
            partition=1;
        }else if(str.equals("pandora")){
            partition=2;
        }else partition=0;

        return partition;
    }
}
