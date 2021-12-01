package mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,SortSpeakerBean, NullWritable> {
    SortSpeakerBean bean =new SortSpeakerBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        bean.setDeviceId(fields[0]);
        bean.setSelfDuration(Long.parseLong(fields[1]));
        bean.setThirdPartDuration(Long.parseLong(fields[2]));
        bean.setSumDuration(Long.parseLong(fields[3]));
        context.write(bean,NullWritable.get());
    }
}
