package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount_Reducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    // 计数器
    int sum =0;
    IntWritable word_Count =new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum=0;
        // 统计单词出现的次数
        for (IntWritable count:values ) {
            sum+=count.get();
            // sum++;
        }
        // 输出
        word_Count.set(sum);
        context.write(key,word_Count);

    }
}
