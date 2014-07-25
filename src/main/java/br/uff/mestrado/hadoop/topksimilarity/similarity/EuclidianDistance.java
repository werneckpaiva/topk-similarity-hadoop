package br.uff.mestrado.hadoop.topksimilarity.similarity;

import br.uff.mestrado.hadoop.topksimilarity.domain.Point;

public class EuclidianDistance {

	public static Double calculate(Point point1, Point point2) {
		if (!point1.getDimension().equals(point2.getDimension())){
			throw new RuntimeException("Points must have the same dimension");
		}
		Double squareSum = 0.0;
		for(int i=0; i<point1.getDimension(); i++){
			squareSum += Math.pow(point1.getValue(i) - point2.getValue(i), 2);
		}
	    Double result = Math.sqrt( squareSum );
	    return result;
	}

}
