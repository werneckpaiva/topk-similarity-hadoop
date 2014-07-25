package br.uff.mestrado.hadoop.topksimilarity.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.PointPair;

public class SingleKeyMapper extends
        Mapper<LongWritable, PointPair, IntWritable, PointPair> {

    protected static Logger _log = LoggerFactory
            .getLogger(SingleKeyMapper.class);

    private IntWritable one = new IntWritable(1);

    public void map(LongWritable key, PointPair pair, Context context)
            throws IOException, InterruptedException {

        context.write(one, pair);
    }

}
