import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        FlowBean bean = new FlowBean();
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        for(FlowBean flowBean:values){
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
            upCountFlow += flowBean.getUpCountFlow();
            downCountFlow += flowBean.getDownCountFlow();
        }
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setUpCountFlow(upCountFlow);
        bean.setDownCountFlow(downCountFlow);

        context.write(key,bean);
    }
}
