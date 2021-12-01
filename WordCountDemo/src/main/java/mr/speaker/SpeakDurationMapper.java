package mr.speaker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakDurationMapper extends Mapper<LongWritable, Text,Text,SpeakBean> {
    Text key_Id =new Text();
    // SpeakBean value_SpeakBean=null;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行文本信息
        String line =value.toString();
        //拆分 \t
        String[] fields = line.split("\t");
        //获取speakbean对应的信息
        String device_id=fields[1];
        Long selfDuration =Long.parseLong(fields[4]);
        Long thirdDuration =Long.parseLong(fields[5]);
        // 创建新的speakbean
        SpeakBean value_SpeakBean = new SpeakBean(device_id, selfDuration, thirdDuration);
        key_Id.set(device_id);
        context.write(key_Id,value_SpeakBean);
    }
}
