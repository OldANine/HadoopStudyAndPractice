package mr.ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class DeliverReduce extends Reducer<Text,DeliverBean,DeliverBean,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<DeliverBean> values, Context context) throws IOException, InterruptedException {
        // 进行join操作
        /*
        主要需求是将投递数据与职位表数据中的职位名称对应起来
        多个投递数据对应同一个职位数据
        将职位数据添加到投递数据中对应处
         */
        /*for (DeliverBean bean : values) {
            context.write(bean,NullWritable.get());
        }*/

        ArrayList<DeliverBean> deBean= new ArrayList<>();
        DeliverBean posBean=new DeliverBean();
        for (DeliverBean bean : values) {
            // context.write(bean,NullWritable.get());
            if("deliver".equalsIgnoreCase(bean.getFlag())){

                //投递数据
                DeliverBean dBean =new DeliverBean();
                // 深度拷贝
                try {
                    BeanUtils.copyProperties(dBean,bean);
                    deBean.add(dBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }else{
                try {
                    BeanUtils.copyProperties(posBean,bean);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

       //合并输出
        for (DeliverBean bean : deBean) {
            bean.setPositionName(posBean.getPositionName());
            context.write(bean, NullWritable.get());
        }

    }
}
