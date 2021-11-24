package com.laojiu.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakDurationReducer extends Reducer<Text,SpeakBean,Text,SpeakBean> {

    String deviceId="";
    Long selfDuration=0l;
    Long thirdDuration =0l;
    Text key_DeviceId=new Text();
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        for (SpeakBean sb:values) {
            deviceId=sb.getDeviceId();
            selfDuration+=sb.getSelfDuration();
            thirdDuration+=sb.getThirdPartDuration();
        }
        SpeakBean speakBean=new SpeakBean(deviceId,selfDuration,thirdDuration);
        key_DeviceId.set(deviceId);
        context.write(key_DeviceId,speakBean);
    }
}
