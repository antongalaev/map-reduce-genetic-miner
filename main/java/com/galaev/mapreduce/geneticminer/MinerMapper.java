package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.deckfour.xes.info.XLogInfo;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:16
 */
public class MinerMapper extends MapReduceBase
        implements Mapper<LongWritable, HeuristicsNetImpl, LongWritable, HeuristicsNetImpl> {

    private static final Logger logger = LoggerFactory.getLogger(MinerMapper.class);
    XLogInfo logInfo = null;

    {
        logger.info("In mapper " + this.toString());
        try {
            logInfo = MinerDriver.getLogInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(LongWritable key, HeuristicsNetImpl value, OutputCollector<LongWritable, HeuristicsNetImpl> output, Reporter reporter) throws IOException {
        SingleFitness fitness = new SingleFitness(logInfo);
        HeuristicsNetImpl individual = (HeuristicsNetImpl) fitness.calculate(value);
        output.collect(key, individual);
    }
}