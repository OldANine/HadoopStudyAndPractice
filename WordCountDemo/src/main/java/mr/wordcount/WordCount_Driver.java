package mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount_Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置文件对象，获取job对象实例
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);;
        // 2. 指定程序jar的本地路径
        job.setJarByClass(WordCount_Driver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(WordCount_Mapper.class);
        job.setReducerClass(WordCount_Reducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6. 指定job处理的原始数据路径与输出结果路径
        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\wordCount\\wc.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\output_wc"));
        // 7. 提交作业
        boolean result = job.waitForCompletion(true);
        // 是否正常退出
        System.exit(result?0:-1);
    }

}

