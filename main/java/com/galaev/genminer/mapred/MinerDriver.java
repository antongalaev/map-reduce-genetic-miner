package com.galaev.genminer.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Driver class for MapReduce Genetic Miner algorithm.
 * Main class in the module, that starts the algorithm in its main method.
 *
 * @see com.galaev.genminer.mapred.MinerDriver
 * @see com.galaev.genminer.mapred.MinerReducer
 * @author Anton Galaev
 */
public class MinerDriver {

    private static final Logger logger = LoggerFactory.getLogger(MinerDriver.class);

    // hdfs path to populations
    public static final String POPULATIONS_PATH = "population" + File.separator + "gen";
    // number of mappers/reducers
    public static final int NUM_REDUCERS = 4;


    // local path to the log
    private String input;
    // local path to the result
    private String output;
    // size of the population
    private int populationSize;
    // number of generations
    private int numGenerations;
    // log info
    private XLogInfo logInfo;
    // start of the algorithm
    private long startTime;
    // elapsed time for each job or stage
    private long[] times;

    // HDFS utilities
    private Configuration conf;
    private FileSystem fs;

    /**
     * Main method, that starts the program.
     * Parses command-line parameters and runs the driver,
     * which, in turn runs hadoop jobs.
     *
     * @param args 0 - main class full name (com.galaev.genminer.mapred.MinerDriver)
     *             1 - input path (to the log),
     *             2 - output path (for final results)
     *             3 - population size
     *             4 - number of generations
     *             5 - start time in millis (optional)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        logger.info("Start timer");
        long start = System.currentTimeMillis();
        // create the driver and parse params
        MinerDriver driver = new MinerDriver();
        driver.setInput(args[1]);
        driver.setOutput(args[2]);
        driver.setPopulationSize(Integer.parseInt(args[3]));
        driver.setNumGenerations(Integer.parseInt(args[4]));
        // parse start time if present
        if (args.length > 5 && ! args[5].equals("")) {
            driver.setStartTime(Long.parseLong(args[5]));
        } else {
            driver.setStartTime(System.currentTimeMillis());
        }
        driver.times = new long[driver.getNumGenerations() + 1];
        // run the algorithm
        driver.run();
        long elapsed = System.currentTimeMillis() - start;
        logger.info(Arrays.toString(driver.times) + "\nElapsed Time: " + elapsed / 1000 + " seconds");
    }

    /**
     * Runs the algorithm.
     * Prepares hdfs for running (copies the log, removes previous results).
     * Creates number of jobs equal to number of generations and runs them
     * sequentially.
     * Copies results to local file system (output path).
     *
     * @throws Exception
     */
    public void run() throws Exception {
        // prepare the initial state for running the algorithm
        prepareHdfs();
        // run jobs
        for (int i = 1; i <= numGenerations; i++) {
            times[i] = System.currentTimeMillis();
            logger.info("--------------------GENERATION #" + i + "---------------------");
            JobConf nextJob = createJob(i);
            JobClient.runJob(nextJob);
            times[i] = System.currentTimeMillis() - times[i];
        }
        // copy results to the output
        copyResultToLocal();
    }

    /**
     * Prepares hdfs for running.
     * Copies the log to HDFS.
     * Removes previous results.
     * Writes initial population as generation #0 to the HDFS.
     *
     * @throws Exception
     */
    private void prepareHdfs() throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(conf);
        copyLogToHdfs();
        writeInitialPopulation();
    }

    /**
     * Copies the log from local file to HDFS.
     * Deletes old results in the HDFS.
     * Extracts log info from the log object.
     *
     * @throws Exception
     */
    private void copyLogToHdfs() throws Exception {
        // copy log to HDFS from local file
        String home = fs.getHomeDirectory().toString() + File.separator;
        fs.copyFromLocalFile(false, true, new Path(input), new Path(home + "log.xes"));
        // delete old results
        fs.delete(new Path(home + "population"), true);
        // read the log info
        logInfo = getLogInfo();
    }

    /**
     * Creates initial population as generation #0.
     * Writes it to the HDFS.
     *
     * @throws Exception
     */
    public void writeInitialPopulation() throws Exception {
        times[0] = System.currentTimeMillis();
        // create the initial population
        GeneticMinerSettings settings = new GeneticMinerSettings();
        settings.setPopulationSize(populationSize);
        Random generator = new Random(settings.getSeed());
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator,
                logInfo, settings.getPower()).build(population);
        // write it to the hdfs (4 files)
        SequenceFile.Writer writers[] = new SequenceFile.Writer[4];
        try {
            for (int i = 0; i < writers.length; i++) {
                Path path = new Path(POPULATIONS_PATH + 0 + File.separator + "init" + i);
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

    /**
     * Copies results to local file system (output path).
     *
     * @throws Exception
     */
    private void copyResultToLocal() throws Exception {
        // read results from the last generation folder
        List<HeuristicsNetImpl> result = new ArrayList<>();
        for (int i = 0; i < NUM_REDUCERS; i++) {
            Path path = new Path(POPULATIONS_PATH + numGenerations + File.separator + "part-0000" + i);

            IntWritable key = new IntWritable();
            HeuristicsNetImpl net = new HeuristicsNetImpl();
            SequenceFile.Reader reader = null;
            try {
                reader = new SequenceFile.Reader(fs, path, conf);
                while (reader.next(key, net)) {
                    result.add(net);
                    net = new HeuristicsNetImpl();
                }
            } finally {
                IOUtils.closeStream(reader);
            }
        }
        // evaluate fitness for the last generation
        for (HeuristicsNetImpl individual : result) {
            SingleNetFitness fitness = new SingleNetFitness(logInfo);
            fitness.calculate(individual);
        }
        // sort
        Collections.sort(result);
        // write the best 100
        Path path = new Path("result_at_" + startTime);
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, HeuristicsNetImpl.class);
            for (int i = 1; i <= 100; i++) {
                HeuristicsNetImpl net = result.get(populationSize - i);
                writer.append(new IntWritable(i), net);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
        // copy the best 100 to local file
        fs.copyToLocalFile(path, new Path(output));
    }

    /**
     * Creates a hadoop job,
     * that serves as one generation.
     *
     * @param i number of the current generation
     * @return created job
     */
    private JobConf createJob(int i) {
        JobConf job = new JobConf(MinerDriver.class);
        job.setJobName("Mining gen #" + i);

        job.set("dfs.blocksize", "1048576");
        job.set("mapreduce.tasktracker.map.tasks.maximum", "4");
        job.set("mapreduce.tasktracker.reduce.tasks.maximum", "4");
        job.setNumMapTasks(NUM_REDUCERS);
        job.setNumReduceTasks(NUM_REDUCERS);
        // set params for mappers
        job.set("populationSplits", String.valueOf(populationSize / (NUM_REDUCERS * 50)));
        job.set("inputLog", fs.getHomeDirectory() + File.separator + "log.xes");

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(POPULATIONS_PATH + (i - 1)));
        FileOutputFormat.setOutputPath(job, new Path(POPULATIONS_PATH + i));
       // job.setPartitionerClass(MinerPartitioner.class);
        job.setMapperClass(MinerMapper.class);
        job.setReducerClass(MinerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(HeuristicsNetImpl.class);
        return job;
    }

    /**
     * Extracts log info from the log.
     * The log must be already copied to the home directory
     * in the HDFS, using the {@code copyLogToHdfs} method.
     *
     * @return log info
     * @throws Exception
     */
    private XLogInfo getLogInfo() throws Exception {
        Path path = new Path(fs.getHomeDirectory() + File.separator + "log.xes");
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

    // Getters and setters

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

    public int getNumGenerations() {
        return numGenerations;
    }

    public void setNumGenerations(int numGenerations) {
        this.numGenerations = numGenerations;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}
