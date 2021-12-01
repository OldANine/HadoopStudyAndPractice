package mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        //设置job对应的设置
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        job.setMapOutputKeyClass(SortSpeakerBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SortSpeakerBean.class);
        //设置 reduceTask数量
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\output_speak"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\output_sort"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
