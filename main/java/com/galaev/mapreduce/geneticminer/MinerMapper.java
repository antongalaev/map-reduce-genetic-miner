package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:16
 */
public class MinerMapper extends Mapper<Object, HeuristicsNetImpl, HeuristicsNetImpl, DoubleWritable> {


    @Override
    protected void map(Object key, HeuristicsNetImpl value, Context context) throws IOException, InterruptedException {

    }
}
