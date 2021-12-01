package mr.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置JDK路径
        job.setJarByClass(PartitionDriver.class);

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        //配置 分区方法
        job.setPartitionerClass(PartitionCustom.class);
        // MapTask任务数量
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\SpeakBean"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\output_partition"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);


    }
}
