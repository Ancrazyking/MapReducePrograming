package mapreduce;

import bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**自定义分区，按手机号前三位进行分区
 * key和value是Map阶段输出的<key,Value>
 * @author afeng
 * @date 2018/11/22 10:04
 **/
public class PhonePartitioner extends Partitioner<Text, FlowBean>
{

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
}
