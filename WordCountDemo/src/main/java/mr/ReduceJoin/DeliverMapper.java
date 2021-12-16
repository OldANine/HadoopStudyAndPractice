package mr.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class DeliverMapper extends Mapper<LongWritable,Text,Text,DeliverBean> {

    private String fileName;
    private DeliverBean bean =new DeliverBean();
    Text k= new Text();

    //获取当前split的文件信息，通过name属性判断文件是deliver或者position
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split=(FileSplit)context.getInputSplit();
        fileName=split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if(fileName.startsWith("deliver_info")){
            //用户投递
            bean.setUserId(split[0]);
            bean.setPositionId(split[1]);
            bean.setDate(split[2]);
            bean.setPositionName("");
            bean.setFlag("deliver");
            k.set(split[1]);
        }else{
            //职位数据
            bean.setUserId("");
            bean.setPositionId(split[0]);
            bean.setDate("");
            bean.setPositionName(split[1]);
            bean.setFlag("position");
            k.set(split[0]);
        }
        context.write(k,bean);
    }
}
