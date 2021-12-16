package mr.ReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DeliverDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(DeliverDriver.class);

        job.setMapperClass(DeliverMapper.class);
        job.setReducerClass(DeliverReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeliverBean.class);
        job.setOutputKeyClass(DeliverBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\reduce_join\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\reduce_join\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);

    }
}
