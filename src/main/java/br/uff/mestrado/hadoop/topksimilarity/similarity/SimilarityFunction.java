package br.uff.mestrado.hadoop.topksimilarity.similarity;

import br.uff.mestrado.hadoop.topksimilarity.domain.Point;

public class SimilarityFunction {
	
	public static Double calculate(Point p1, Point p2){
		return EuclidianDistance.calculate(p1, p2);
	}

}
