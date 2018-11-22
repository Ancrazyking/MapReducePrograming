# MapReducePrograming
During Learning Hadoop MapReduce,Coding Some mapreduce demo

> #### Shuffle阶段说明

![shuffle.png](http://wx4.sinaimg.cn/mw690/006pTdaLgy1fxgkh8t1jvj30mv0aojsb.jpg)


<p style="text-indent:2em">
Shuffle阶段主要包括<i>map阶段</i>的<b>combine、group、sort、partition</b>以及<i>reduce</i>阶段的<b>合并排序</b>。
</p>


<p style="text-indent:2em">
Map阶段通过Shuffle后会将输出数据按照reduce的分区文件保存，文件内容是按照定义的sort进行排序的。
</p>

<p style="text-indent:2em">
Map阶段完成后会通知ApplicationMaster，然后AM会通知Reduce进行数据拉取，在拉取过程中进行<b>reduce端的shuffle过程</b>。
</p>

##### 注意
Map阶段的输出数据是保存在运行Map节点的磁盘上，是临时文件，不存在HDFS上，在Reduce拉取数据后，临时文件会被删除。


> #### 自定义Combiner

<p style="text-indent:2em">
Combiner可以减少Map阶段的中间输出结果，常用与单个任务的局部汇总。
用户自定义Combiner要求是Reducer的子类，并且Combiner的输入和输出kv必须与Mapper阶段的输出KV一致，也就是说Combiner的输入kv与输出kv一致。
</p>


<p style="text-indent:2em">
Combiner主要用于减少Map阶段的中间输出结果，节省网络开销，优化Shuffle过程。
</p>

```
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

# Driver
    //设置CombinerClass为WordCountCombiner.class
        job.setCombinerClass(WordCountCombiner.class);
        
        
```




> #### 自定义分区Partitioner
```
# Partitioner


    /**
     * 手机号码字典
     */
    public static HashMap<String,Integer> phoneDict=new HashMap<String, Integer>();
    static{
        phoneDict.put("136",0);
        phoneDict.put("137",1);
        phoneDict.put("138",2);
        phoneDict.put("139",3);
    }
    /**
     * 获取分区的区号
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    public int getPartition(Text text, FlowBean flowBean, int numPartitions)
    {
        //从下标0开始切，3个字符
        String key=text.toString().substring(0,3);
        Integer phoneReduceId=phoneDict.get(key);
        return phoneReduceId==null?4:phoneReduceId;
    }


# Driver:
        //设置自定义的数据分区器
        job.setPartitionerClass(PhonePartitioner.class);

        //设置Reduce分区数量
        job.setNumReduceTasks(5);

```



> #### 自定义Group
<p style="text-indent:2em">
GroupComparatro是用于将Map输出的<key,value>进行分组组合成<key,List<value>>,就是用于确定key1和key2是否属于同一组，如果是同一组，就将map的输出value进行组合。
</p>
<p style="text-indent:2em">
需要自定义类实现接口RawComoparator，通过Driver中的job.setGroupingComparatorClass方法指定比较类。默认情况下使用WritableComparator,最终调用key的compareTo方法进行比较。
</p>


> #### 自定义Sort


<p style="text-indent:2em">
SortComparator是用于将Map输出的<key,value>进行key排序的关键类，用于确定哪个key在前，那个可以在后。
</p>

<p style="text-indent:2em">
需要自定义类实现接口RawComoparator，通过Driver中的job.setSortComparatorClass方法指定比较类。默认情况下使用WritableComparator,最终调用key的compareTo方法进行比较。
</p>



> #### 自定义Reduce的Shuffle

<p style="text-indent:2em">
Shuffle过程在reduce端拉取map的输出数据的时候，会进行排序(归并排序)，MapReduce框架以插件模式提供了一个自定义的方式，我们可以通过实现接口<b>ShuffleConsumerPlugin</b>,并指定参数mapreduce.job.reduce.shuffle.consumer.plugin.class来指定自定义的shuffle规则。一般情况下，会使用默认类org.apache.hadoop.mapreduce.task.reduce.Shuffle。(归并排序)
</p>

