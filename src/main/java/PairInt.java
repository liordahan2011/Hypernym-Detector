import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairInt implements WritableComparable {
    private IntWritable index;
    private IntWritable sum;

    public PairInt(int index, int sum) {
        this.index = new IntWritable(index);
        this.sum = new IntWritable(sum);
    }

    public PairInt() {
        this.index = new IntWritable(-1);
        this.sum = new IntWritable(-1);
    }

    public int getIndex() {
        return index.get();
    }

    public void setIndex(int index) {
        this.index = new IntWritable(index);
    }

    public int getSum() {
        return sum.get();
    }

    public void setSum(int sum) {
        this.sum = new IntWritable(sum);
    }

    @Override
    public int compareTo(Object o) {
        PairInt other = (PairInt)(o);
        int indexComp = index.compareTo(other.index);
        if(indexComp == 0){
            return this.sum.compareTo(other.sum);
        }
        return indexComp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        index.write(dataOutput);
        sum.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        index.readFields(dataInput);
        sum.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "index= " + index +
                " sum= " + sum;
    }
}
