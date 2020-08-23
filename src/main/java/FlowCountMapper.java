import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text ,Text ,FlowBean> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将传入的值进行切割
        String[] split =  value.toString().split("\t");

        //2.分割手机号
        String phoneNum = split[1];

        //3.获取其余字段
        FlowBean bean = new FlowBean();
        bean.setUpFlow(Integer.parseInt(split[6]));
        bean.setDownFlow(Integer.parseInt(split[7]));
        bean.setUpCountFlow(Integer.parseInt(split[8]));
        bean.setDownCountFlow(Integer.parseInt(split[9]));

        //4.将K2 V2写入上下文中

        context.write(new Text(phoneNum),bean);
    }
}
