package com.laojiu.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsClientIODemo {

    FileSystem fs = null;
    Configuration conf=null;
    public String path_HdfsFolder="/test_IO";
    public String path_testFile="D:/TestFolders/test.txt";
    public String path_testDownloadFolder="D:/TestFolders/Download";

    @Before
    public void init() throws IOException {
        conf= new Configuration();
        // conf.set("dfs.client.use.datanode.hostname","true");
        conf.set("fs.defaultFS","hdfs://linux121:9000");
        fs=FileSystem.get(conf);
    }
    @After
    public void destroy() throws IOException {
        fs.close();
    }


    //使用API创建对应的目录
    public void mkdirs() throws IOException {
        fs.mkdirs(new Path(path_HdfsFolder));
    }

    @Test
    // 上传文件到HDFS 使用IO流进行传输
    public void putFileToHdfs() throws IOException {
        // 本地文件的输入流
        FileInputStream fis=new  FileInputStream(new File(path_testFile));
        // HDFS 文件输出流
        FSDataOutputStream fos = fs.create(new Path(path_HdfsFolder+"/io_test.txt"));
        IOUtils.copyBytes(fis,fos,conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
@Test
    // 使用IO从HDFS中下载文件到本地
    public void getFileFromHdfs() throws IOException {
        // HDfS 文件输入流
        FSDataInputStream fis =fs.open(new Path(path_HdfsFolder+"/io_test.txt"));
        // 本地文件输出流
        FileOutputStream fos = new FileOutputStream((new File(path_testDownloadFolder+"/io_copy.txt")));

        IOUtils.copyBytes(fis,fos,conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
@Test
    //seek定位读取
    // 设置读取的偏移量，从文件的某一位置开始读取指定的长度
    public void readFileSeek(){
        FSDataInputStream fis=null;
        try {
            // 通过输入流获取文件的内容
            fis = fs.open(new Path(path_HdfsFolder+"/io_test.txt"));
            // 通过输出流进行输出
            IOUtils.copyBytes(fis,System.out,4096,false);
            System.out.println("-----------------分割线-----------------");
            // 重置 seek 偏移量为 0
            fis.seek(10);
            IOUtils.copyBytes(fis,System.out,1024,false);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fis);
        }

    }

    /*
    总结：
    文件IO流不够熟悉

    输出流与输入流的补充：
    不管输入或者输出，都是针对本机进行定义的
    输出： 指的是通过本机外部资源获取 对外的， 【外交部】
    输入： 指的是 获取本机资源  对内的，

     */

}
