package com.galaev.genminer.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.*;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.InitialPopulationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:59
 */
public class MinerDriver {

    private static final Logger logger = LoggerFactory.getLogger(MinerDriver.class);

    public static final String POPULATIONS_PATH = "population/gen";
    public static final String HDFS_HOME = "hdfs://localhost:9000/user/anton/";
    public static final int NUM_POPULATIONS = 10;
    public static final int NUM_REDUCERS = 4;



    private String input;
    private String output;
    private int populationSize;
    private static long[] times = new long[NUM_POPULATIONS + 1];

    public static void main(String[] args) throws Exception {
        logger.info("Start timer");
        long start = System.currentTimeMillis();
        MinerDriver driver = new MinerDriver();
        driver.setInput(args[1]);
        driver.setOutput(args[2]);
        driver.setPopulationSize(Integer.parseInt(args[3]));
        driver.run();
        long elapsed = System.currentTimeMillis() - start;
        logger.info("Elapsed Time: " + elapsed / 1000 + " seconds\n" + Arrays.toString(times));

    }

    public void run() throws Exception {
        copyLogToHdfs();

        writeInitialPopulation();

        for (int i = 1; i <= NUM_POPULATIONS; i++) {
            times[i] = System.currentTimeMillis();
            logger.info("--------------------GENERATION #" + i);
            JobConf nextJob = createJob(i);
            JobClient.runJob(nextJob);
            times[i] = System.currentTimeMillis() - times[i];
        }

        copyResultToLocal();
    }

    private void copyLogToHdfs() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(false, true, new Path(input), new Path(HDFS_HOME + "log.xes"));
        fs.delete(new Path(HDFS_HOME + "population"), true);
    }

    private void copyResultToLocal() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output + "/gen" + NUM_POPULATIONS), true);
        fs.copyToLocalFile(new Path(POPULATIONS_PATH + NUM_POPULATIONS), new Path(output));
        fs.close();
    }

    public void writeInitialPopulation() throws Exception {
        times[0] = System.currentTimeMillis();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        XLogInfo logInfo = getLogInfo();
        GeneticMinerSettings settings = new GeneticMinerSettings();
        settings.setPopulationSize(populationSize);
        Random generator = new Random(settings.getSeed());
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator,
                logInfo, settings.getPower()).build(population);

        SequenceFile.Writer writers[] = new SequenceFile.Writer[4];
        try {
            for (int i = 0; i < writers.length; i++) {
                Path path = new Path(POPULATIONS_PATH + 0 + "/init" + i);
                writers[i] = SequenceFile.createWriter(fs, conf, path, IntWritable.class, HeuristicsNetImpl.class);
            }
            for (int i = 0; i < population.length; ++i) {
                IntWritable key = new IntWritable(i);
                ((HeuristicsNetImpl) population[i]).setKey(i);
                writers[i % 4].append(key, population[i]);
            }
        } finally {
            for (SequenceFile.Writer writer : writers) {
                IOUtils.closeStream(writer);
            }
        }
        times[0] = System.currentTimeMillis() - times[0];
    }


    private JobConf createJob(int i) {
        JobConf conf = new JobConf(MinerDriver.class);
        conf.setJobName("Mining gen #" + i);

        conf.set("dfs.blocksize", "1048576");
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "4");
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", "4");
        conf.setNumMapTasks(NUM_REDUCERS);
        conf.setNumReduceTasks(NUM_REDUCERS);
        conf.set("populationSplits", String.valueOf(populationSize / (NUM_REDUCERS * 50)));

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(conf, new Path(POPULATIONS_PATH + (i - 1)));
        FileOutputFormat.setOutputPath(conf, new Path(POPULATIONS_PATH + i));

        conf.setPartitionerClass(MinerPartitioner.class);
        conf.setMapperClass(MinerMapper.class);
        conf.setReducerClass(MinerReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(HeuristicsNetImpl.class);
        return conf;
    }

    public static XLogInfo getLogInfo() throws Exception {
        Path path = new Path(HDFS_HOME + "log.xes");
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream in = null;
        try {
            XesXmlParser parser = new XesXmlParser();
            in = fs.open(path);
            List<XLog> logs = parser.parse(in);
            XLog log = logs.get(0);
            return XLogInfoFactory.createLogInfo(log);
        } finally {
            IOUtils.closeStream(in);
        }
    }


    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public int getPopulationSize() {
        return populationSize;
    }

    public void setPopulationSize(int populationSize) {
        this.populationSize = populationSize;
    }
}
