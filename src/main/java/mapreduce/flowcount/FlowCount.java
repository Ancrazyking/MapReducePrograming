package mapreduce.flowcount;

import bean.FlowBean;
import mapreduce.PhonePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 用于统计流量
 * 手机号和流量汇总
 * 手机号相同按照总流量大小降序排列
 *
 * @author afeng
 * @date 2018/11/22 10:10
 **/
public class FlowCount
{

    /**
     * Driver
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //设置自定义的数据分区器
        job.setPartitionerClass(PhonePartitioner.class);

        //设置Reduce分区数量
        job.setNumReduceTasks(5);

        //值得Mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //指定job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //获取结果
        //0正常退出，1非正常退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }


    /**
     * Mapper    Map阶段
     * (Long,String)  => (String,FlowBean)
     */
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>
    {
        FlowBean flowBean = new FlowBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] fields = line.split("\t");
            Integer upFlow = Integer.parseInt(fields[fields.length - 2]);
            Integer dFlow = Integer.parseInt(fields[fields.length - 3]);
            String phoneNum = fields[1];
            flowBean.setUpFlow(upFlow);
            flowBean.setdFlow(dFlow);
            flowBean.set(upFlow, dFlow);
            k.set(phoneNum);
            context.write(k, flowBean);
        }
    }

    /**
     * Reducer,聚合阶段
     * (String,List<FlowBean>) => (String,FlowBean)
     */
    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>
    {

        FlowBean flowBean = new FlowBean();
        Text k = new Text();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException
        {
            int upFlow = 0;
            int dFlow = 0;
            for (FlowBean v : values)
            {
                upFlow += v.getUpFlow();
                dFlow += v.getdFlow();
            }
            flowBean.set(upFlow, dFlow);
            k.set(key);
            context.write(key, flowBean);
        }
    }


}
