package mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable,Text,Text,PartitionBean> {
    PartitionBean bean = new PartitionBean();
    Text text=new Text();
    String appkey;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        appkey=fields[2];
        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);
        text.set(appkey);
        context.write(text,bean);
    }
}
