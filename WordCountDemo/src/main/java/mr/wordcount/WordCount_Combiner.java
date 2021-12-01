package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
Combiner 组件


 */

public class WordCount_Combiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    int total =0;
    IntWritable iw=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        total=0;
        for (IntWritable value : values) {
            total+=value.get();
        }
        iw.set(total);
        context.write(key,iw);
    }
}
