package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author afeng
 * @date 2018/11/19 17:43
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int totalSum = 0;
        for (IntWritable value : values)
        {
            totalSum += value.get();
        }
        v.set(totalSum);
        context.write(key, v);
    }
}
