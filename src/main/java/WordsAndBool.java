import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordsAndBool implements WritableComparable {
    private Text word1;
    private Text word2;
    private BooleanWritable subType;

    public WordsAndBool(String word1, String word2, Boolean subType) {
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
        if (subType == null)
            this.subType = null;
        else
            this.subType = new BooleanWritable(subType);
    }

    public WordsAndBool() {
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.subType = new BooleanWritable(false);
    }

    @Override
    public int compareTo(Object o) {
        WordsAndBool other = (WordsAndBool)o ;
        int word1Comp = this.word1.compareTo(other.word1);
        if(word1Comp == 0){
            int word2Comp = this.word2.compareTo(other.word2);
            if(word2Comp == 0){
                return this.subType.compareTo(other.subType);
            }
            return word2Comp;
        }
        return word1Comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        if (subType != null)
            subType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        if (subType != null)
            subType.readFields(dataInput);
    }

    @Override
    public String toString() {
        String bool = String.valueOf(subType);
        if (subType == null)
            bool = "?";
        return word1 +
                " " + word2 +
                " " + bool;
    }
}
