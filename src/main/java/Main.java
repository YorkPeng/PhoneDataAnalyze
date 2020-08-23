import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration,new Main(),args));
    }

    public int run(String[] strings) throws Exception {
        //创建一个任务
        Job job = Job.getInstance(super.getConf(),"mapreduce_flowcount_partition");

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(Main.class);
        //1.设置读取文件的类：K1和V1
        job.setInputFormatClass(TextInputFormat.class);
        //设置输入路径
        TextInputFormat.addInputPath(job,new Path("input/flowcount"));

        //2. 设置mapper类
        job.setMapperClass(FlowCountMapper.class);
        //3.设置map阶段输出类型，即k2,v2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //4.设置分区
        job.setPartitionerClass(FlowPartition.class);

        //5.设置reducer
        job.setReducerClass(FlowCountReducer.class);

        //6.设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //7.设置reduce的个数
        job.setNumReduceTasks(4);

        //8.设置恶输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        //9.设置输出路径
        TextOutputFormat.setOutputPath(job,new Path("output/flowcount"));
        return job.waitForCompletion(true)?0:1;
    }
}
