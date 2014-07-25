package br.uff.mestrado.hadoop.topksimilarity.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements Writable, WritableComparable<Point>, Cloneable {

    private Long index;
    private Long pid;

    private Integer dimension;
    private Double[] values;

    public Point(Integer dimension) {
        this.setIndex(index);
        this.dimension = dimension;
        values = new Double[dimension];
        for (int i = 0; i < dimension; i++) {
            values[i] = 0.0;
        }
    }

    public Point() {
        this(2);
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Integer getDimension() {
        return dimension;
    }

    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    public Double getValue(int i) {
        return this.values[i];
    }

    public void setValue(int i, Double value) {
        this.values[i] = value;
    }

    public void readFields(DataInput in) throws IOException {
        dimension = in.readInt();
        values = new Double[dimension];
        for (int i = 0; i < dimension; i++) {
            values[i] = in.readDouble();
        }
        index = in.readLong();
        pid = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(dimension);
        for (int i = 0; i < dimension; i++) {
            out.writeDouble(values[i]);
        }
        out.writeLong(index);
        out.writeLong(pid);
    }

    /**
     * Return the Euclidean distance from origin
     **/
    public Double distanceFromOrigin() {
        Double squareSum = 0.0;
        for (int i = 0; i < dimension; i++) {
            squareSum += values[i] * values[i];
        }
        Double result = Math.sqrt(squareSum);
        return result;
    }

    /**
     * Return the Euclidean distance from the point
     **/
    public Double distanceFromPoint(Point point) {
        Double squareSum = 0.0;
        for (int i = 0; i < dimension; i++) {
            squareSum += Math.pow(values[i] - point.getValue(i), 2);
        }
        Double result = Math.sqrt(squareSum);
        return result;
    }

    public int compareTo(Point other) {
        Double myDistance = distanceFromOrigin();
        Double otherDistance = other.distanceFromOrigin();

        return Double.compare(myDistance, otherDistance);
    }

    @Override
    public String toString() {
        return serialize();
    }

    @Override
    public Point clone() {
        Point p = new Point(dimension);
        p.values = this.values.clone();
        p.index = this.index;
        p.pid = this.pid;
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Point))
            return false;
        Point p = (Point) o;
        return this.compareTo(p) == 0;
    }

    public String serialize() {
        StringBuffer sb = new StringBuffer();
        if (index != null) {
            sb.append(index);
            sb.append(":");
        }
        sb.append("(");
        String comma = "";
        for (int i = 0; i < dimension; i++) {
            sb.append(comma);
            sb.append(this.getValue(i));
            comma = ", ";
        }
        sb.append(")");
        return sb.toString();
    }

    public static Point unserialize(String strPoint) {
        String[] parts = strPoint.split(":");
        Long index = null;
        if (parts != null && parts.length == 2) {
            index = Long.valueOf(parts[0]);
            strPoint = parts[1];
        }
        strPoint = StringUtils.remove(strPoint, "(");
        strPoint = StringUtils.remove(strPoint, ")");
        String[] numbers = strPoint.split(", *");
        Point pt = new Point(numbers.length);
        pt.index = index;
        for (int i = 0; i < numbers.length; i++) {
            pt.setValue(i, Double.valueOf(numbers[i]));
        }
        return pt;
    }

}
