import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairStrings implements WritableComparable {
    private Text word1;
    private Text word2;

    public PairStrings(String word1, String word2) {
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
    }

    public PairStrings() {
        this.word1 = new Text("");
        this.word2 = new Text("");
    }

    public String getWord1() {
        return word1.toString();
    }

    public void setWord1(String word1) {
        this.word1 = new Text(word1);
    }

    public String getWord2() {
        return word2.toString();
    }

    public void setWord2(String word2) {
        this.word2 = new Text(word2);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if(!(obj instanceof PairStrings)){
            return false;
        }
        PairStrings other = (PairStrings)obj;
        return this.word1.equals(other.word1) && this.word2.equals(other.word2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word1, word2);
    }

    @Override
    public String toString() {
        return "word1= " + word1.toString() +
                " word2= " + word2.toString();
    }

    @Override
    public int compareTo(Object o) {
        PairStrings other = (PairStrings)o;
        int w1Comp = word1.compareTo(other.word1);
        if(w1Comp == 0){
            return word2.compareTo(other.word2);
        }
        return w1Comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
    }
}
