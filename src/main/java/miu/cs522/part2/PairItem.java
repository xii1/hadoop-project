package miu.cs522.part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author XIII
 */
public class PairItem implements WritableComparable<PairItem> {

    private Text first;
    private Text second;

    public PairItem() {
        first = new Text();
        second = new Text();
    }

    public PairItem(String first, String second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public int compareTo(PairItem o) {
        final int k = this.first.compareTo(o.first);

        if (k != 0) return k;
        else return this.second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairItem pairItem = (PairItem) o;
        return Objects.equals(first, pairItem.first) && Objects.equals(second, pairItem.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return String.format("(%s -> %s)", first.toString(), second.toString());
    }
}
