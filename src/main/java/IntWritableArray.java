import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntWritableArray implements Writable {
    private IntWritable[] arr;

    public IntWritableArray(int[] arr) {
        this.arr = new IntWritable[arr.length];
        for(int i=0; i< arr.length; i++){
            this.arr[i] = new IntWritable(arr[i]);
        }
    }
    public IntWritableArray() {
        arr = null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i=0; i<arr.length; i++){
            arr[i].write(dataOutput);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for(int i=0; i<arr.length; i++){
            arr[i].readFields(dataInput);
        }
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
//        res.append("[");  // open
        String separator = " ";
        for (IntWritable i : arr){
            res.append(i.get());
            res.append(separator); // separator
        }
        res.substring(0,res.length() - separator.length());
//        res.append("]") ;// close
        return res.toString();
    }
}
