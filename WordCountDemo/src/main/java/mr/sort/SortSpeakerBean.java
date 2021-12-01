package mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortSpeakerBean implements WritableComparable<SortSpeakerBean> {

    private String deviceId;
    private Long selfDuration;
    private Long thirdPartDuration;
    private Long sumDuration;

    public SortSpeakerBean() {
    }

    public SortSpeakerBean(String deviceId,Long selfDuration, Long thirdPartDuration,  Long sumDuration) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = sumDuration;
    }

    // 重写比较方式
    // 按照升序进行排序
    @Override
    public int compareTo(SortSpeakerBean o) {
        if(this.sumDuration > o.sumDuration){
            return 1;
        }else if(this.sumDuration<o.sumDuration){
            return -1;
        }else return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(deviceId);
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.deviceId=in.readUTF();
        this.selfDuration=in.readLong();
        this.thirdPartDuration=in.readLong();
        this.sumDuration=in.readLong();
    }

    @Override
    public String toString() {
        return  deviceId + '\t' +
                selfDuration + "\t" +
                thirdPartDuration + "\t" +
                sumDuration;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

}
