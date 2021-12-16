package mr.MapJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DeliverMapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Text k =new Text();
    Map<String,String > map=new HashMap<>();

    //将职位表中的数据缓存到Map中存放，通过拼接的形式将职位名称合并
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存的文件
        BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("position.txt"),"UTF-8"));  //D:\TestFolders\join\map\cache\
        // BufferedReader reader = new BufferedReader(new InputStreamReader(new
                // FileInputStream("position.txt"),"UTF-8"));

        // FileInputStream fileInputStream = new FileInputStream("position.txt");
        // InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        // BufferedReader reader =new BufferedReader(inputStreamReader);

        // 使用管理员方式可以只用文件名称
        // 如果不是需要使用全路径方式指定文件

        String line;
        while (StringUtils.isNotEmpty(line=reader.readLine())){
            String[] fields = line.split("\t");
            map.put(fields[0],fields[1]);
        }
        reader.close();
    }

    // 正常的读取投递行为数据，根据id进行职位名称的拼接并直接输出
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\t");
        String name = map.get(fields[1]);
        k.set(line+"\t"+name);
        context.write(k,NullWritable.get());
    }
}
