package mr.ReduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeliverBean implements Writable {

    private String userId;
    private String positionId;
    private String date;
    private String positionName;
    private String flag;    //区分文件属性  deliver 与 position

    public DeliverBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(positionId);
        out.writeUTF(date);
        out.writeUTF(positionName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId=in.readUTF();
        positionId=in.readUTF();
        date=in.readUTF();
        positionName=in.readUTF();
        flag=in.readUTF();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return  userId + '\t' +
                positionId + '\t' +
                positionName+'\t'  +
                date;
    }
}
