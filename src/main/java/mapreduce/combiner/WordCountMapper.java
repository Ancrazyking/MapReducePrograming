package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author afeng
 * @date 2018/11/19 17:43
 **/
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
    Text k=new Text();
    IntWritable v=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line=value.toString();
        String[] words=line.split(",");
        for(String word:words){
            k.set(word);
            context.write(k,v);
        }
    }
}
