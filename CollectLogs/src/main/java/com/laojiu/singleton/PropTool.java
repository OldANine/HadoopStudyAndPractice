package com.laojiu.singleton;

import com.laojiu.collectLogs.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

public class PropTool {

    /*
    * volatile 关键字
    * java中禁止指令重排序的关键字
    * 防止在编译过程中程序被重排， --> 异常
    * 保证 有序性和可见性
    *
    * */
    public static volatile Properties prop=null;

    public static Properties getProp() throws IOException {
        // synchronized ("lock") {
        //     if (prop == null) {
        //         prop = new Properties();
        //         prop.load(LogCollectorTask.class.getClassLoader().getResourceAsStream("Collectorlog.properties"));
        //     }
        // }

        // 极致化
        if(prop==null) {
            synchronized ("lock") {
                prop = new Properties();
                prop.load(LogCollectorTask.class.getClassLoader().getResourceAsStream("Collectorlog.properties"));
            }
        }
        return prop;
    }
}

/*
    多线程
    线程安全性
    线程的性能

 */
