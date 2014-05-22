package com.galaev.genminer.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

/**
 * Mapper class for MapReduce Genetic Miner algorithm.
 * Evaluates fitness for every net, that comes inside,
 * then assigns a new key to the net. Whole population
 * is divided on several splits. So the mapper generates
 * the split number for an individual.
 *
 * @see com.galaev.genminer.mapred.MinerDriver
 * @see com.galaev.genminer.mapred.MinerReducer
 * @author Anton Galaev
 */
public class MinerMapper extends MapReduceBase
        implements Mapper<IntWritable, HeuristicsNetImpl, IntWritable, HeuristicsNetImpl> {

    private static final Logger logger = LoggerFactory.getLogger(MinerMapper.class);

    // number of splits for population
    private int populationSplits;
    // extracted log info
    private XLogInfo logInfo;
    private Random generator;

    /**
     * Evaluates fitness for every net, that comes inside,
     * then assigns a new key to the net. Whole population
     * is divided on several splits, so the function generates
     * the split number for an individual.
     *
     * @param key heuristic net number (key)
     * @param value heuristic net
     * @param output collector
     * @param reporter reporter
     * @throws IOException
     */
    @Override
    public void map(IntWritable key, HeuristicsNetImpl value, OutputCollector<IntWritable, HeuristicsNetImpl> output, Reporter reporter) throws IOException {
        // create fitness object
        SingleNetFitness fitness = new SingleNetFitness(logInfo);
        // evaluate fitness for current individual
        HeuristicsNetImpl individual = (HeuristicsNetImpl) fitness.calculate(value);
        // generate split number for individual
        int newKey = generator.nextInt(populationSplits);
        output.collect(new IntWritable(newKey), individual);
    }

    /**
     * Configures the mapper.
     * Extracts log info,
     * sets the number of splits for population.
     *
     * @param job current job
     */
    @Override
    public void configure(JobConf job) {
        logger.info("In mapper " + this.toString());
        try {
            logInfo = getLogInfo(job.get("inputLog"));
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        generator = new Random(1);
        populationSplits = Integer.parseInt(job.get("populationSplits"));
    }

    /**
     * Extracts log info from the log.
     *
     * @return log info
     * @throws Exception
     */
    private XLogInfo getLogInfo(String input) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(input);
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
}