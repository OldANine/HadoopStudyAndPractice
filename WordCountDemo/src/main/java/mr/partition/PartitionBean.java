package mr.partition;

import org.apache.hadoop.io.Writable;
import sun.management.jdp.JdpException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionBean implements Writable {
    // 日志文件对应的属性
    private String id;
    private String deviceId;
    private String appkey;
    private String ip;
    private Long selfDuration;
    private Long thirdPartDuration;
    private String status;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(status);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id=in.readUTF();
        deviceId=in.readUTF();
        appkey=in.readUTF();
        ip=in.readUTF();
        selfDuration=in.readLong();
        thirdPartDuration=in.readLong();
        status=in.readUTF();
    }

    public PartitionBean() {
    }

    public PartitionBean(String id, String deviceId, String appkey, String ip, Long selfDuration, Long thirdPartDuration, String status) {
        this.id = id;
        this.deviceId = deviceId;
        this.appkey = appkey;
        this.ip = ip;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return  id + '\t' +
                deviceId + '\t' +
                appkey + '\t' +
                ip + '\t' +
                selfDuration + "\t" +
                thirdPartDuration + "\t" +
                status;
    }
}
