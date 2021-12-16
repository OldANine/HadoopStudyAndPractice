package mr.GroupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf= new Configuration();
        Job job= Job.getInstance(conf);
        //jon运行基础
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);
        //Map输入输出的key、value
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //文件输出的key、value
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //输入、输出文件路径信息
        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\GroupingComparator"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\output_GroupingComparator"));
        //指定分区器
        job.setPartitionerClass(OrderPartitioner.class);
        //指定分组比较器
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        //指定reduceTask数量
        job.setNumReduceTasks(3);
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
