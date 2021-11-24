package com.laojiu.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
自定义对象，继承 Hadoop的Writable接口

重写序列化与反序列化方法
Map阶段时将对应的信息Reduce到同一个key内
重写 toString 读取时更便利

 */

public class SpeakBean implements Writable {
    //对应属性字段
    private String deviceId;    // 设备ID
    private long selfDuration;  // 自有内容时长
    private long thirdPartDuration; //第三方内容时长
    private long sumDuration;   // 总时长

    // 无参构造方法
    public SpeakBean() {
    }
    //有参构造方法 总时长=自有时长+第三方时长
    public SpeakBean(String deviceId, long selfDuration, long thirdPartDuration) {
        this.deviceId = deviceId;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.sumDuration=selfDuration+thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(long sumDuration) {
        this.sumDuration = sumDuration;
    }

    //重写序列化方法
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(deviceId);
        output.writeLong(selfDuration);
        output.writeLong(thirdPartDuration);
        output.writeLong(sumDuration);
    }

    //重写反序列方法
    @Override
    public void readFields(DataInput input) throws IOException {
        deviceId= input.readUTF();
        selfDuration=input.readLong();
        thirdPartDuration =input.readLong();
        sumDuration=input.readLong();
    }

    @Override
    public String toString() {
        return /* deviceId + '\t' +*/
                selfDuration + "\t" +
                thirdPartDuration + "\t" +
                sumDuration;
    }
}
