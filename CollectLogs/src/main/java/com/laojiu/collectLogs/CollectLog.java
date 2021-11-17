package com.laojiu.collectLogs;

/*
    实现采集日志程序的主入口
    主要功能：
    1.采集本地日志，并按照日期进行分类存放
    2.将分类完成的日志上传到HDFS中
    3.复制相应的文件到本地备份目录中


 */

import java.util.Timer;

public class CollectLog {

    public static void main(String[] args) {
        Timer timer =new Timer();
        timer.schedule(new LogCollectorTask(),0,3600*1000);
    }
}
