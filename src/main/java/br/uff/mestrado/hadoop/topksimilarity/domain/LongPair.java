package br.uff.mestrado.hadoop.topksimilarity.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongPair implements Writable, WritableComparable<LongPair>,
        Cloneable {

    private Long value1;
    private Long value2;

    public LongPair(Long value1, Long value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public LongPair() {
    }

    public Long getValue1() {
        return value1;
    }

    public void setValue1(Long value1) {
        this.value1 = value1;
    }

    public Long getValue2() {
        return value2;
    }

    public void setValue2(Long value2) {
        this.value2 = value2;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        sb.append(value1.toString());
        sb.append(", ");
        sb.append(value2.toString());
        sb.append(")");
        return sb.toString();
    }

    public int compareTo(LongPair otherPair) {
        int ret = value1.compareTo(otherPair.getValue1());
        if (ret == 0) {
            ret = value2.compareTo(otherPair.getValue2());
        }
        return ret;
    }

    public void readFields(DataInput in) throws IOException {
        this.value1 = in.readLong();
        this.value2 = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(value1);
        out.writeLong(value2);
    }

}
