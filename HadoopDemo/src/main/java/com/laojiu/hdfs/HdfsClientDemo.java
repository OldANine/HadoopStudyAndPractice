package com.laojiu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClientDemo {

    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 获取 Hadoop 集群的configuration 对象
        Configuration con = new Configuration();
        // 根据configuration 对象获取 filesystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),con,"root");
        // 使用filesystem对象创建目录
        fs.mkdirs(new Path("/Mkdir-api"));
        // 释放资源
        fs.close();

        //正常运行，在HDFS中创建目录
    }

    public void testMkdirs_2() throws IOException {
        Configuration con =new Configuration();
        con.set("fs.defaultFS","hdfs://linux121:9000");
        FileSystem fs = FileSystem.get(con);
        fs.mkdirs(new Path("/ApiMkdir-con"));
        fs.close();

        // 运行报错，权限问题
    /*
    *
    * org.apache.hadoop.security.AccessControlException: Permission denied:
    * user=Administrator, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
    *   HDFS 文件系统权限问题
    *   1. 指定用户信息获取 FileSystem 对象，不指定，默认使用 Administrator 用户
    *   2. 关闭HDFS集群权限校验
    *       vim hdfs-site.xml
            #添加如下属性
            <property>
                <name>dfs.permissions</name>
                <value>true</value>
            </property>
    *   3. 修改 HDFS 根目录权限为 777
    *
    */
    }

    FileSystem fs =null;
    @Before
    // 获取HDFS文件的初始化的工作
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf =new Configuration();
        conf.set("dfs.client.use.datanode.hostname","true");
        //设置副本数量为 1
        // conf.set("dfs.replication","1");
        fs =FileSystem.get(new URI("hdfs://linux121:9000"),conf,"root");
    }
    @After
    // 释放资源流
    public void destroy() throws IOException {
        fs.close();
    }

    //使用API创建目录
    public void testMkdirsSimple() throws IOException {
        fs.mkdirs(new Path("/APIMkdir"));
    }

    //上传文件
    public void copyFromLocalToHdfs() throws IOException {
        // src: 源文件
        //dst： hdfs路径
        fs.copyFromLocalFile(new Path("D:/TestFiles/test.txt"),new Path("/test_1.txt"));

        // 出现无法访问 datanode 的通信问题
        // 解决 https://www.cnblogs.com/krcys/p/9146329.html
        /*
            在configuration 中设置通过域名访问 datanode
            configuration.set("dfs.client.use.datanode.hostname", "true");
        */
    }

    // 测试上传文件时副本数量的设置
    public void testFileReplication() throws IOException {
        /*
        * 测试副本数量的设置及优先级
        * 1.最高级 代码中设置，
        * Configuration.set("dfs.replication","1")
        * 2. hdfs-site.xml 配置文件中
        *     <property>
                <name>dfs.replication</name>
                <value>2</value>
               </property>
           3. 服务器的默认配置
        *
        * */

        // 测试一 在代码中进行设置 设置副本数量为 1
        // fs.copyFromLocalFile(new Path("D:/TestFiles/test.txt"),new Path("/APIMkdir/copytest_java"));
        // 查看 副本数量为 1

        // 测试二 通过xml配置文件获取 设置副本数量为 2
        fs.copyFromLocalFile(new Path("D:/TestFiles/test.txt"),new Path("/APIMkdir/copytest_XML"));
        //此时副本数量为 2
    }

    //从hdfs中下载文件到本地
    public void copyFromHdfsTolocal() throws IOException {
        fs.copyToLocalFile(new Path("/APIMkdir/test.txt"),new Path("D:\\TestFiles\\Download\\copy.txt"));
    }

    // 删除文件或文件夹
    public void deletFiles() throws IOException {
        // 文件路径信息，是否遍历
        fs.delete(new Path("/Mkdir-api"),true);
    }

    // 遍历 / 目录下所有文件的详细信息 名称、权限、大小
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);    //返回的是迭代器类型
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            //文件名称
            String name = status.getPath().getName();
            //文件大小
            long len = status.getLen();
            //文件权限
            FsPermission permission = status.getPermission();
            //分组信息
            String group = status.getGroup();
            // 所属
            String owner = status.getOwner();
            System.out.println("文件名："+name+"\t"+"大小："+len+"\t"+"权限："+permission+"\t"+"用户组："+group+"\t"+"用户："+owner);
            // 存储的块Block信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host:hosts) {
                    System.out.println("主机名："+host);
                }
            }
            System.out.println("-------------------------------");
        }
    }

    // 文件夹与文件判断
    public void isFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
         for (FileStatus status : listStatus) {
             System.out.println("-------------------------------");
             String name = status.getPath().getName();
             boolean flag=status.isFile();
             System.out.println("当前的"+(flag?"文件":"文件夹")+"为："+ name +"\t");
        }
    }
}
