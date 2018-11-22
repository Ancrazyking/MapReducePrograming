package bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义的value，需要实现Hadoop中的序列化
 *
 * @author afeng
 * @date 2018/11/22 9:46
 **/
public class FlowBean implements WritableComparable<FlowBean>
{

    private long upFlow;//上行流量
    private long dFlow;//下行流量
    private long sumFlow;//总流量

    public FlowBean()
    {
    }

    public FlowBean(long upFlow, long dFlow, long sumFlow)
    {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }


    /**
     * 二次排序
     * 升序还是降序
     * 降序吧
     *
     * @param o
     * @return
     */
    public int compareTo(FlowBean o)
    {
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }

    /**
     * 序列化写数据
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 序列化读数据
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException
    {
        upFlow = dataInput.readLong();
        dFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    public long getUpFlow()
    {
        return upFlow;
    }

    public void setUpFlow(long upFlow)
    {
        this.upFlow = upFlow;
    }

    public long getdFlow()
    {
        return dFlow;
    }

    public void setdFlow(long dFlow)
    {
        this.dFlow = dFlow;
    }

    public long getSumFlow()
    {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow)
    {
        this.sumFlow = sumFlow;
    }

    /**
     * 设置使得sumFlow为上行流量和下行流量之和
     *
     * @param upFlow
     * @param dFlow
     */
    public void set(long upFlow, long dFlow)
    {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        sumFlow = upFlow + dFlow;
    }


    @Override
    public String toString()
    {
        return upFlow+"\t"+dFlow+"\t"+sumFlow;
    }
}
