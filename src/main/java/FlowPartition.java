import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<Text,FlowBean> {
    public int getPartition(Text text, FlowBean flowBean, int i) {
        if(text.toString().startsWith("135")){
            return 0;
        }else if(text.toString().startsWith("136")){
            return 1;
        }else if(text.toString().startsWith("137")){
            return 2;
        }else {
            return 3;
        }
    }
}
