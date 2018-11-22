package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author afeng
 * @date 2018/11/19 17:43
 * <p>
 * Combiner是在map阶段对MapTask任务进行局部汇总
 **/
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {

        int sum = 0;
        for (IntWritable value : values)
        {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
