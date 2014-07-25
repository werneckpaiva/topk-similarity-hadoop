package br.uff.mestrado.hadoop.topksimilarity.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.LongPair;
import br.uff.mestrado.hadoop.topksimilarity.domain.Point;

public class PointToPairMapper extends
        Mapper<LongWritable, Point, LongPair, Point> {

    protected static Logger _log = LoggerFactory
            .getLogger(PointToPairMapper.class);

    protected Integer m;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        m = context.getConfiguration().getInt("m", 5);
    }

    public void map(LongWritable index, Point pointValue, Context context)
            throws IOException, InterruptedException {
        long pid = (index.get() % (m)) + 1; // [1, m]

        for (long i = 1; i <= m; i++) {
            if (i <= pid) {
                context.write(new LongPair(i, pid), pointValue);
            } else {
                context.write(new LongPair(pid, i), pointValue);
            }
        }

    }
}
