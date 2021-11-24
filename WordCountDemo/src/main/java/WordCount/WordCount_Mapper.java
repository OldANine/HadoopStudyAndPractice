package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCount_Mapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    // Mapper 输出的key与value
    private Text keyWord =new Text();
    private IntWritable int_Tag =new IntWritable(1);    //该值为固定的任意值

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行文本数据
        String line=value.toString();
        //按照空格进行拆分为单个单词
        String[] words=line.split(" ");
        //遍历数组 words 进行输出
        // 输出格式为 <key,value> key为单词，value为固定值
        for (String word : words) {
            keyWord.set(word);
            context.write(keyWord,int_Tag);
        }
    }
}
