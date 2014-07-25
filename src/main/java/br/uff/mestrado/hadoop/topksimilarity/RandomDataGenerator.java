package br.uff.mestrado.hadoop.topksimilarity;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Calendar;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.uff.mestrado.hadoop.topksimilarity.domain.Point;


public class RandomDataGenerator {

    private enum OutputFileType{
        HADOOP_SEQUENCE,
        HADOOP_TEXT,
        TEXT
    }

    protected static Logger log = LoggerFactory.getLogger(RandomDataGenerator.class);

    private static Options cmdOptions;

    private Configuration conf;

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {

        // Command line options
        cmdOptions = new Options();
        cmdOptions.addOption(new Option("create", false, "Create Points. Requires dimension, number of points and path"));
        cmdOptions.addOption(new Option("listarray", true, "List array file"));
        cmdOptions.addOption(new Option("listsequence", true, "List sequence file"));
        cmdOptions.addOption("d", "dimension", true, "Point dimension");
        cmdOptions.addOption("f", "files", true, "Number of files");
        cmdOptions.addOption("n", "number", true, "Number of points to generate");
        cmdOptions.addOption("o", "output", true, "Output file used by -create and -join");
        cmdOptions.addOption("s", "sequence", false, "Use hadoop sequence file");
        cmdOptions.addOption("h", "hdfs", false, "Use hadoop HDFS");

        CommandLineParser parser = new PosixParser();
        try {
            CommandLine cmd = parser.parse(cmdOptions, args);

            // Generate numbers
            if (cmd.hasOption("create") 
                    && cmd.hasOption("dimension")
                    && cmd.hasOption("number")
                    && cmd.hasOption("output")) {
                Integer dimension = Integer.valueOf(cmd.getOptionValue("dimension"));
                Long numPoints = Long.valueOf(cmd.getOptionValue("number"));
                String hadoopOutputFile = cmd.getOptionValue("output");
                Long files = 1L;
                if (cmd.hasOption("files")){
                    files = Long.valueOf(cmd.getOptionValue("files"));
                }
                OutputFileType outputFileType;
                if (cmd.hasOption("sequence")){
                    outputFileType =  OutputFileType.HADOOP_SEQUENCE;
                } else if (cmd.hasOption("hdfs")){
                    outputFileType =  OutputFileType.HADOOP_TEXT;
                } else {
                    outputFileType =  OutputFileType.TEXT;
                }
                RandomDataGenerator generator = new RandomDataGenerator();
                generator.generatePoints(dimension, numPoints, files, hadoopOutputFile, outputFileType);

                return;

            // List Numbers in array files
            } else if (cmd.hasOption("listarray") ){
                String hadoopFile = cmd.getOptionValue("listarray");
                RandomDataGenerator generator = new RandomDataGenerator();
                generator.listContentFromArrayFile(hadoopFile);

                return;

            // List Numbers in sequence files
            } else if (cmd.hasOption("listsequence") ){
                String hadoopFile = cmd.getOptionValue("listsequence");
                RandomDataGenerator generator = new RandomDataGenerator();
                generator.listContentFromSequenceFile(hadoopFile);

                return;

            }
            RandomDataGenerator.printCommandHelp();
        } catch( ParseException exp ) {
            // something went wrong
            System.err.println( "Wrong parameters" + exp.getMessage() );
            printCommandHelp();
        }
    }

    private static void printCommandHelp(){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "RandomDataGenerator", cmdOptions);
        System.exit(1);
    }

    public RandomDataGenerator(){
        conf = new Configuration();
    }

    private void generatePoints(Integer dimension, Long numPoints, Long numFiles, String hadoopOutputFile, OutputFileType outputFileType) throws IOException{

        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer sequenceFile = null;

        PrintStream textOut = null;

        Long pointsPerFile = (numPoints / numFiles);
        Long index = 0L;
        for (Long numFile = 0L; numFile < numFiles; numFile++){
            try {
                // Creating file
                String fileName = hadoopOutputFile+numFile;
                System.out.println("Creating "+fileName);
                Path hadoopOutputFilePath = new Path(fileName);
                if (outputFileType == OutputFileType.HADOOP_SEQUENCE){
                    sequenceFile = new SequenceFile.Writer(fs, conf, hadoopOutputFilePath, LongWritable.class, Point.class);
                } else if (outputFileType == OutputFileType.HADOOP_TEXT){
                    OutputStream out = fs.create(hadoopOutputFilePath);
                                    textOut = new PrintStream(new BufferedOutputStream(out));
                } else {
                    OutputStream out = new FileOutputStream(fileName);
                    textOut = new PrintStream(new BufferedOutputStream(out));
                }

                // Generating points
                Point point = null;
                Double randomValue = null;

                Random generator = new Random(Calendar.getInstance().getTimeInMillis());
//                int sum = 2;
                for (Long i = 0L; i < pointsPerFile; i++) {
                    point = new Point(dimension);
                    point.setIndex(index);
                    for (int j = 0; j < dimension; j++){
                        randomValue = Double.valueOf(generator.nextDouble());
                        point.setValue(j, randomValue);
//                        point.setValue(j, Double.valueOf(i * sum));
                    }
                    index++;
                    if (outputFileType == OutputFileType.HADOOP_SEQUENCE){
                        sequenceFile.append(new LongWritable(index), point);
                    } else {
                        textOut.println(point.serialize());
                    }
//                    sum+=2;
                    if ((index % 1000000) == 0) System.out.print(".");
                }
                System.out.println("");
            } catch (Exception e){
                e.printStackTrace();
                return;
            } finally {
                    if (outputFileType == OutputFileType.HADOOP_SEQUENCE){
                    IOUtils.closeStream(sequenceFile);
                } else if (textOut != null){
                    textOut.close();
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private void listContentFromSequenceFile(String hadoopFile) throws IOException, InstantiationException, IllegalAccessException {
        SequenceFile.Reader reader = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            Path filePath = new Path(hadoopFile);
            reader = new SequenceFile.Reader(fs, filePath, conf);
            
            WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();
            
            while(reader.next(key,value)) {
                System.out.println(key.toString() + ": " + value.toString());
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    private void listContentFromArrayFile(String hadoopFile) throws IOException, InstantiationException, IllegalAccessException {
        ArrayFile.Reader reader = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            reader = new ArrayFile.Reader(fs, hadoopFile, conf);
            
            LongWritable key = new LongWritable();
            Writable value = (Writable) reader.getValueClass().newInstance();
            
            while(reader.next(key,value)) {
                System.out.println(key.toString() + ": " + value.toString());
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }


//    private void joinPoints(String hadoopInputFile, String hadoopOutputFile) throws IOException, InstantiationException, IllegalAccessException {
//        ArrayFile.Reader arrayFileReader1 = null;
//        ArrayFile.Reader arrayFileReader2 = null;
//        SequenceFile.Writer arrayFileWriter = null;
//        try {
//            FileSystem fs = FileSystem.get(conf);
//            arrayFileReader1 = new ArrayFile.Reader(fs, hadoopInputFile, conf);
//            arrayFileReader2 = new ArrayFile.Reader(fs, hadoopInputFile, conf);
//            
//            Path hadoopOutputFilePath = new Path(hadoopOutputFile);
//            
//            arrayFileWriter = new SequenceFile.Writer(fs, conf, hadoopOutputFilePath, Point.class, Point.class);
//            
//            LongWritable key1 = new LongWritable();
//            LongWritable key2 = new LongWritable();
//            
//            Point pointkey = new Point();
//            Point pointValue = new Point();
//            
//            while(arrayFileReader1.next(key1, pointkey)) {
//                arrayFileReader2.seek(key1.get());
//                while(arrayFileReader2.next(key2, pointValue)) {
//                    arrayFileWriter.append(pointkey, pointValue);
//                }
//                System.out.print(".");
//            }
//            System.out.println("");
//        } finally {
//            IOUtils.closeStream(arrayFileReader1);
//            IOUtils.closeStream(arrayFileReader2);
//            IOUtils.closeStream(arrayFileWriter);
//        }
//    }
    
}
