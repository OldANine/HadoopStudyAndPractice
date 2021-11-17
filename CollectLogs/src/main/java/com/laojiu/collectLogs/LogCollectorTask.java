package com.laojiu.collectLogs;

import com.laojiu.common.Constant;
import com.laojiu.singleton.PropTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.IFileOutputStream;
import org.mortbay.util.IO;
import sun.misc.IOUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogCollectorTask extends TimerTask {
    @Override
    public void run() {

        // 获取配置文件 Properties
        Properties prop = null;
        try {
            prop = PropTool.getProp();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 日志采集
        // 按照日期进行分类
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); //年-月-日的形式
        String dataFormat = sdf.format(new Date());
        //
        File logDir = new File(prop.getProperty(Constant.LOG_DIR));
        // 获取指定的文件内容
        final String log_prefix = prop.getProperty(Constant.LOG_PREFIXE);
        File[] uploadFiles = logDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(log_prefix);
            }
        });
        //筛选指定的文件

        /*
        ArrayList<File> uploadFiles =new ArrayList<>();
        for (File listFile : listFiles) {
            // System.out.println("-------"+listFile.getName());
            if(listFile.getName().startsWith(prop.getProperty(Constant.LOG_PREFIXE))){
                uploadFiles.add(listFile);
            }
        }
        */

        //移动筛选后的文件到临时目录
        File tempFile = new File(prop.getProperty(Constant.LOG_TMP_FOLDER));
        if (!tempFile.exists()) {
            tempFile.mkdirs();
        }
        for (File file : uploadFiles) {
            file.renameTo(new File(tempFile.getPath() + "/" + file.getName()));
            // try {
            //     FileInputStream fis = new FileInputStream(file);
            //     FileOutputStream fos =new FileOutputStream(
            //             new File(tempFile.getPath()+"/"+file.getName()));
            //     IO.copy(fis,fos);
            //
            //
            // } catch (FileNotFoundException e) {
            //     e.printStackTrace();
            // } catch (IOException e) {
            //     e.printStackTrace();
            // } finally {
            //
            // }

        }

        // 上传到HDFS 与备份目录中
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://linux121:9000");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path_Hdfs = new Path(prop.getProperty(Constant.HDFS_TARGET_FOLDER) + dataFormat);
            // System.out.println(path_Hdfs.toString());
            // 如果HDFS中不存在目录，创建对应的目录
            if (!fs.exists(path_Hdfs)) {
                fs.mkdirs(path_Hdfs);
            }
            //备份目录
            File bakFolder = new File(prop.getProperty(Constant.BAK_FOLDER) + "/" + dataFormat);
            if (!bakFolder.exists()) {
                bakFolder.mkdirs();
            }

            //上传文件
            // System.out.println(uploadFiles.length+"\t------------------");
            File[] files = tempFile.listFiles();
            for (File file : files) {
                // System.out.println("----------"+file.getPath().toString());
                //上传到HDFS中
                fs.copyFromLocalFile(new Path(file.getPath()), new Path(
                        prop.getProperty(Constant.HDFS_TARGET_FOLDER) + dataFormat + "/" + file.getName()));
                //保存到备份目录中
                file.renameTo(new File(bakFolder.getPath() + "/" + file.getName()));
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
