package com.galaev.genminer.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;

import java.util.Random;

/**
 * Custom partitioner for MapReduce Genetic Miner algorithm.
 * Reducer number is chosen randomly.
 *
 * @author Anton Galaev
 */
public class MinerPartitioner implements Partitioner<IntWritable, HeuristicsNetImpl> {

    private Random random = new Random(1);

    @Override
    public int getPartition(IntWritable key, HeuristicsNetImpl net, int numReducers) {
        return random.nextInt(numReducers);
    }

    @Override
    public void configure(JobConf entries) {

    }
}
