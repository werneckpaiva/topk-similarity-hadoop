package br.uff.mestrado.hadoop.topksimilarity.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.PointPair;

public class TopKReducer extends
        Reducer<IntWritable, PointPair, IntWritable, PointPair> {

    private static Logger _log = LoggerFactory.getLogger(TopKReducer.class);

    public void reduce(IntWritable key, Iterable<PointPair> pairs,
            Context context) throws IOException, InterruptedException {

        Integer k = context.getConfiguration().getInt("k", 1);

        Iterator<PointPair> pairsIterator = pairs.iterator();
        PointPair pair = null;
        for (int i = 0; i < k; i++) {
            pair = pairsIterator.next();
            _log.debug("i: {} pair: {}", i, pair);
            context.write(new IntWritable(i), pair);
        }
    }

}
