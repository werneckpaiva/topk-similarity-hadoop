package br.uff.mestrado.hadoop.topksimilarity.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PointPair implements Writable, WritableComparable<PointPair>,
        Cloneable {

    private Point pointA;
    private Point pointB;

    private Double similarity;

    public PointPair(Point pointA, Point pointB, Double similarity) {
        this.pointA = pointA;
        this.pointB = pointB;
        this.similarity = similarity;
    }

    public PointPair() {

    }

    public Point getPointA() {
        return pointA;
    }

    public void setPointA(Point pointA) {
        this.pointA = pointA;
    }

    public Point getPointB() {
        return pointB;
    }

    public void setPointB(Point pointB) {
        this.pointB = pointB;
    }

    public Double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(Double similarity) {
        this.similarity = similarity;
    }

    /**
     * Return the Euclidean distance from the two points
     **/
    public Double distance() {
        return pointA.distanceFromPoint(pointB);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        sb.append(pointA.toString());
        sb.append(", ");
        sb.append(pointB.toString());
        sb.append(" s: ");
        sb.append(similarity);
        sb.append("}");
        return sb.toString();
    }

    public int compareTo(PointPair otherPair) {
        Double thisDistance = this.distance();
        Double otherDistance = otherPair.distance();
        return thisDistance.compareTo(otherDistance);
    }

    public void readFields(DataInput in) throws IOException {
        this.pointA = new Point();
        this.pointB = new Point();
        this.pointA.readFields(in);
        this.pointB.readFields(in);
        this.similarity = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        pointA.write(out);
        pointB.write(out);
        out.writeDouble(similarity);
    }

    @Override
    public PointPair clone() {
        PointPair p = new PointPair(pointA.clone(), pointB.clone(), similarity);
        return p;
    }

}
