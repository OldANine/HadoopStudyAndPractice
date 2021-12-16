package mr.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DeliverMapJoinDriver  {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        job.setJarByClass(DeliverMapJoinDriver.class);

        job.setMapperClass(DeliverMapJoinMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\TestFolders\\join\\map\\in"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\TestFolders\\join\\map\\output"));

        job.addCacheFile(new URI("file:///D:/TestFolders/join/map/cache/position.txt"));
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
