import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 存储一对Text对象的Writable
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }


    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        return (first.hashCode() * 163 + second.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextPair) {
            TextPair tp = (TextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    /**
     * 用于比较TextPair字节表示的RawComparator
     */
    public static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new
                Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);//第一个字节流中第一个Text字段长度
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2); //第二个字节流中第一个Text字段长度

                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        static {
            WritableComparator.define(TextPair.class, new Comparator());// 设置TextPair的类型比较器
        }
    }

    /**
     * 定制的RawComparator 用于比较TextPair对象字节表示的第一个字段
     */
    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {

                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);//第一个字节流中第一个Text字段长度
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2); //第二个字节流中第一个Text字段长度

                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            }catch (IOException e){
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if(a instanceof TextPair && b instanceof TextPair){
                return ((TextPair)a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
}
