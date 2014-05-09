package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 09/05/2014
 * Time: 14:38
 */
public class MinerPartitioner implements Partitioner<HeuristicsNetImpl, DoubleWritable> {

    Random random = new Random(1);


    @Override
    public int getPartition(HeuristicsNetImpl key, DoubleWritable value, int numReducers) {
        return random.nextInt(numReducers);
    }

    @Override
    public void configure(JobConf entries) {

    }
}
