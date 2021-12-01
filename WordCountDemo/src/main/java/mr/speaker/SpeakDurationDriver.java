package mr.speaker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakDurationDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置路径信息
        // args=new String[]{"D:/TestFolders/SpeakBean","D:/TestFolders/output"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //jdk
        job.setJarByClass(SpeakDurationDriver.class);
        //map/reduce
        job.setMapperClass(SpeakDurationMapper.class);
        job.setReducerClass(SpeakDurationReducer.class);
        //key/value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        //文件目录
        FileInputFormat.setInputPaths(job,new Path("D:/TestFolders/SpeakBean"));
        FileOutputFormat.setOutputPath(job,new Path("D:/TestFolders/output_speak"));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
