package br.uff.mestrado.hadoop.topksimilarity;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.LongPair;
import br.uff.mestrado.hadoop.topksimilarity.domain.Point;
import br.uff.mestrado.hadoop.topksimilarity.domain.PointPair;
import br.uff.mestrado.hadoop.topksimilarity.mapper.PointToPairMapper;
import br.uff.mestrado.hadoop.topksimilarity.mapper.PointToPairMapperText;
import br.uff.mestrado.hadoop.topksimilarity.mapper.SingleKeyMapper;
import br.uff.mestrado.hadoop.topksimilarity.reducer.TopKDistanceReducer;
import br.uff.mestrado.hadoop.topksimilarity.reducer.TopKReducer;

public class TopKBruteForceApp {
    protected static Logger _log = LoggerFactory
            .getLogger(TopKBruteForceApp.class);

    private Configuration conf;
    private String inputFile;
    private String outputFolder;

    private Job jobTopK;
    private Job jobFinalMerge;

    private Boolean useSequenceFile;

    private static Options cmdOptions;

    public static void main(String[] args) {

        // Command line options
        cmdOptions = new Options();
        cmdOptions.addOption("k", true, "Top k parameter");
        cmdOptions.addOption("m", true, "Number of reduces");
        cmdOptions.addOption("s", false, "Use hadoop sequence file");
        cmdOptions.addOption("i", "input", true, "Input");
        cmdOptions.addOption("o", "output", true, "Output");

        CommandLineParser parser = new PosixParser();
        try {
            CommandLine cmd = parser.parse(cmdOptions, args);

            if (cmd.hasOption('k') && cmd.hasOption('m') && cmd.hasOption('i')
                    && cmd.hasOption('o')) {
                Integer k = Integer.valueOf(cmd.getOptionValue('k'));
                Integer m = Integer.valueOf(cmd.getOptionValue('m'));
                String inputFolder = cmd.getOptionValue('i');
                String outputFolder = cmd.getOptionValue('o');
                Boolean useSequenceFile = cmd.hasOption('s');

                TopKBruteForceApp app = new TopKBruteForceApp(k, m,
                        inputFolder, outputFolder, useSequenceFile);
                try {
                    app.runJobs();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(3);
                }
                return;
            }

            TopKBruteForceApp.printCommandHelp();

        } catch (ParseException exp) {
            // something went wrong
            System.err.println("Wrong parameters" + exp.getMessage());
            printCommandHelp();
        }
    }

    private static void printCommandHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TopKBruteForceApp", cmdOptions);
        System.exit(1);
    }

    public TopKBruteForceApp(Integer k, Integer m, String inputFile,
            String outputFolder, Boolean useSequenceFile) {
        this.conf = new Configuration();
        this.conf.setInt("k", k);
        this.conf.setInt("m", m);
        this.useSequenceFile = useSequenceFile;
        this.inputFile = inputFile;
        this.outputFolder = outputFolder;
    }

    public void runJobs() throws Exception {
        Boolean ok;

        _log.info("Removing output folder");
        FileSystem fs = FileSystem.get(this.conf);
        fs.delete(new Path(this.outputFolder), true);

        _log.info("Configuring job TopK");
        configureJobTopK();
        _log.info("Running TopK MapReduce");
        ok = jobTopK.waitForCompletion(true);
        if (!ok)
            throw new Exception("TopKBruteForce job failed");

        // moveOutputFiles();

        _log.info("Configuring job FinalMerge");
        configureJobFinalMerge();
        _log.info("Running FinalMerge");
        ok = jobFinalMerge.waitForCompletion(true);
        if (!ok)
            throw new Exception("TopKBruteForce job failed");
    }

    private void configureJobTopK() throws IOException {
        this.conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ":");

        jobTopK = new Job(conf, "RunTopK");
        jobTopK.setJarByClass(TopKBruteForceApp.class);

        if (this.useSequenceFile) {
            jobTopK.setInputFormatClass(SequenceFileInputFormat.class);
            jobTopK.setMapperClass(PointToPairMapper.class);
        } else {
            jobTopK.setInputFormatClass(KeyValueTextInputFormat.class);
            jobTopK.setMapperClass(PointToPairMapperText.class);
        }

        jobTopK.setMapOutputKeyClass(LongPair.class);
        jobTopK.setMapOutputValueClass(Point.class);

        int m = conf.getInt("m", 1);
        int numReducers = (m*(m+1)) / 2;
        jobTopK.setReducerClass(TopKDistanceReducer.class);
        jobTopK.setNumReduceTasks(numReducers);

        jobTopK.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobTopK.setOutputKeyClass(LongWritable.class);
        jobTopK.setOutputValueClass(PointPair.class);

        FileInputFormat.addInputPath(jobTopK, new Path(inputFile));
        FileOutputFormat.setOutputPath(jobTopK, new Path(outputFolder
                + Path.SEPARATOR + "partial"));

    }

    private void configureJobFinalMerge() throws IOException {
        jobFinalMerge = new Job(conf, "RunFinalMerge");
        jobFinalMerge.setJarByClass(TopKBruteForceApp.class);

        jobFinalMerge.setInputFormatClass(SequenceFileInputFormat.class);
        jobFinalMerge.setMapperClass(SingleKeyMapper.class);

        jobFinalMerge.setMapOutputKeyClass(IntWritable.class);
        jobFinalMerge.setMapOutputValueClass(PointPair.class);

        jobFinalMerge.setReducerClass(TopKReducer.class);

        // jobFinalMerge.setOutputFormatClass(SequenceFileOutputFormat.class);
        // jobFinalMerge.setOutputKeyClass(IntWritable.class);
        // jobFinalMerge.setOutputValueClass(PointPair.class);

        FileInputFormat.addInputPath(jobFinalMerge, new Path(outputFolder
                + Path.SEPARATOR + "partial/part*"));
        FileOutputFormat.setOutputPath(jobFinalMerge, new Path(outputFolder
                + Path.SEPARATOR + "final"));
    }

//    @Deprecated
//    private List<PointPair> mergeArrays(List<PointPair> listA,
//            List<PointPair> listB, final int K) {
//        List<PointPair> result = new ArrayList<PointPair>();
//        int ptA = 0, ptB = 0;
//        PointPair pointPairA, pointPairB;
//        for (int i = 1; i <= K; i++) {
//            if (ptA >= listA.size() && ptB >= listB.size()) {
//                break;
//            }
//            // Did listA finish?
//            if (ptA >= listA.size()) {
//                // _log.debug("Adding from A: {}", listB.get(ptB));
//                result.add(listB.get(ptB));
//                ptB++;
//                continue;
//            }
//            // Did listB finish?
//            if (ptB >= listB.size()) {
//                // _log.debug("Adding from B: {}", listA.get(ptA));
//                result.add(listA.get(ptA));
//                ptA++;
//                continue;
//            }
//            pointPairA = listA.get(ptA);
//            pointPairB = listB.get(ptB);
//            int compareAandB = pointPairA.compareTo(pointPairB);
//            // Is pointPairA less than pointPairB
//            if (compareAandB < 0) {
//                // _log.debug("Adding from A: {}", pointPairA);
//                result.add(pointPairA);
//                ptA++;
//            } else if (compareAandB > 0) {
//                result.add(pointPairB);
//                // _log.debug("Adding from B: {}", pointPairB);
//                ptB++;
//            } else {
//                result.add(pointPairA);
//                // _log.debug("Adding from both: {}", pointPairA);
//                ptA++;
//                ptB++;
//            }
//        }
//        return result;
//    }
//
//    @Deprecated
//    private void mergeResultFiles() throws IOException {
//        FileSystem fs = FileSystem.get(conf);
//        FileStatus[] fss = fs.listStatus(new Path(this.outputFolder));
//
//        List<PointPair> result = new ArrayList<PointPair>();
//        List<PointPair> currentList = new ArrayList<PointPair>();
//        Integer k = conf.getInt("k", 2);
//
//        Long rank = 0L;
//        // For each file
//        for (FileStatus status : fss) {
//            Path path = status.getPath();
//            if (path.getName().indexOf("part") < 0) {
//                continue;
//            }
//            _log.debug("file: {}", path.getName());
//            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
//            LongWritable key = new LongWritable();
//            PointPair pair = new PointPair();
//            while (reader.next(key, pair)) {
//                // _log.debug("similarity: {} pair: {}", pair.getSimilarity(),
//                // pair);
//                if (key.get() < rank) {
//                    result = mergeArrays(result, currentList, k);
//                    currentList = new ArrayList<PointPair>();
//                }
//                rank = key.get();
//                currentList.add(pair.clone());
//            }
//            reader.close();
//        }
//        SequenceFile.Writer resultFileWriter = null;
//        try {
//            Path outputFile = new Path(this.outputFolder + "/result");
//            resultFileWriter = new SequenceFile.Writer(fs, conf, outputFile,
//                    LongWritable.class, PointPair.class);
//
//            int i = 1;
//            for (PointPair pointPair : result) {
//                if (i > k)
//                    break;
//                resultFileWriter.append(new LongWritable(i), pointPair);
//                _log.debug("i: {} pair: {}", i, pointPair);
//                i++;
//            }
//        } finally {
//            IOUtils.closeStream(resultFileWriter);
//        }
//    }
//
//    @Deprecated
//    private void moveOutputFiles() throws IOException {
//        FileSystem fs = FileSystem.get(conf);
//        FileStatus[] fss = fs.listStatus(new Path(this.outputFolder
//                + Path.SEPARATOR + "partial"));
//
//        // For each file
//        for (FileStatus status : fss) {
//            Path path = status.getPath();
//            if (path.getName().indexOf("part") < 0) {
//                fs.delete(path, true);
//            }
//        }
//    }
}
