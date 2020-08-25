import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class Trio implements WritableComparable {

    // fields
    private Text route;
    private Text word1;
    private Text word2;

    public Trio(String route,String word1, String word2) {
        this.route = new Text(route);
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
    }

    public Trio() {
        this.route = new Text("");
        this.word1 = new Text("");
        this.word2 = new Text("");
    }

    public String getRoute() {
        return route.toString();
    }

    public void setRoute(Text route) {
        this.route = route;
    }

    public String getWord1() {
        return word1.toString();
    }

    public void setWord1(Text word1) {
        this.word1 = word1;
    }

    public String getWord2() {
        return word2.toString();
    }

    public void setWord2(Text word2) {
        this.word2 = word2;
    }

    @Override
    public int compareTo(Object o) {
        Trio other = (Trio)o;
        int routeCompare = this.route.compareTo(other.route);
        if(routeCompare == 0){
            int w1Compare = this.word1.compareTo(other.word1);
            if(w1Compare == 0){
                return this.word2.compareTo(other.word2);
            }
            return w1Compare;
        }
        return routeCompare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        route.write(dataOutput);
        word1.write(dataOutput);
        word2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        route.readFields(dataInput);
        word1.readFields(dataInput);
        word2.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "route= " + getRoute() +
                " word1= " + getWord1() +
                " word2= " + getWord2();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trio trio = (Trio) o;
        return route.equals(trio.route) &&
                word1.equals(trio.word1) &&
                word2.equals(trio.word2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(route, word1, word2);
    }
}
