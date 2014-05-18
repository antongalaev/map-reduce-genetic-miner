package com.galaev.genminer.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.deckfour.xes.info.XLogInfo;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:16
 */
public class MinerMapper extends MapReduceBase
        implements Mapper<IntWritable, HeuristicsNetImpl, IntWritable, HeuristicsNetImpl> {

    private static final Logger logger = LoggerFactory.getLogger(MinerMapper.class);

    private int populationSplits;
    private Random generator = new Random(1);
    private XLogInfo logInfo = null;

    {
        logger.info("In mapper " + this.toString());
        try {
            logInfo = MinerDriver.getLogInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    @Override
    public void map(IntWritable key, HeuristicsNetImpl value, OutputCollector<IntWritable, HeuristicsNetImpl> output, Reporter reporter) throws IOException {
        SingleFitness fitness = new SingleFitness(logInfo);
        HeuristicsNetImpl individual = (HeuristicsNetImpl) fitness.calculate(value);
        int newKey = generator.nextInt(populationSplits);
        output.collect(new IntWritable(newKey), individual);
    }

    @Override
    public void configure(JobConf job) {
        populationSplits = Integer.parseInt(job.get("populationSplits"));
    }
}