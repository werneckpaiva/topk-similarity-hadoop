package br.uff.mestrado.hadoop.topksimilarity.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.LongPair;
import br.uff.mestrado.hadoop.topksimilarity.domain.Point;
import br.uff.mestrado.hadoop.topksimilarity.domain.PointPair;
import br.uff.mestrado.hadoop.topksimilarity.similarity.SimilarityFunction;

public class TopKDistanceReducer extends
        Reducer<LongPair, Point, LongWritable, PointPair> {

    private static Logger _log = LoggerFactory
            .getLogger(TopKDistanceReducer.class);

    private Integer k;

    @Override
    public void setup(Context context) {
        k = context.getConfiguration().getInt("k", 1);
    }

    public void reduce(LongPair key, Iterable<Point> points, Context context)
            throws IOException, InterruptedException {

        SortedMap<Double, PointPair> sortedMap = new TreeMap<Double, PointPair>();

        // Se o Pid1 e Pid2 forem iguais, fazer todas as combinacoes
        // Se o Pid1 e Pid2 forem diferentes, fazer o produto cartesiano entre os pontos com Pid1 e os pontos com Pid2

        List<Point> pointLists_i = new ArrayList<Point>();
        List<Point> pointLists_j = new ArrayList<Point>();

        // Set the cartesian product between points with Pid1 and points with Pid2
        for (Point point : points) {
            // _log.debug("{} - {}", key, point);
            if (point.getPid().equals(key.getValue1())) {
                pointLists_i.add(point.clone());
            }
            if (point.getPid().equals(key.getValue2())) {
                pointLists_j.add(point.clone());
            }
        }

        // _log.debug("size(i): {} / size(j): {}", pointLists_i.size(),
        Point point1, point2;
        int startJ = 0;
        for (int i = 0; i < pointLists_i.size(); i++) {
            point1 = pointLists_i.get(i);
            startJ = 0;
            if (key.getValue1().equals(key.getValue2())) {
                startJ = i + 1;
            }
            for (int j = startJ; j < pointLists_j.size(); j++) {
                point2 = pointLists_j.get(j);
                if (point1.equals(point2)) {
                    continue;
                }
                Double similarity = SimilarityFunction
                        .calculate(point1, point2);
                PointPair pair = new PointPair(point1, point2, similarity);
                PointPair savedPair = sortedMap.get(similarity);
                if (savedPair != null && savedPair.compareTo(pair) == 0) {
                    continue;
                }
                // _log.debug("Pair: {}", pair);
                sortedMap.put(similarity, pair);
                if (sortedMap.size() > k) {
                    sortedMap.remove(sortedMap.lastKey());
                }
            }
        }

        int i = 1;
        for (Map.Entry<Double, PointPair> entry : sortedMap.entrySet()) {
            if (i > k) {
                break;
            }
            _log.debug("i: {} pair: {}", i, entry.getValue());
            context.write(new LongWritable(i), entry.getValue());
            i++;
        }
    }
}
