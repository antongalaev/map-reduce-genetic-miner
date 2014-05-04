package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:16
 */
public class MinerMapper extends Mapper<Object, HeuristicsNetWritable, HeuristicsNetWritable, DoubleWritable> {


    @Override
    protected void map(Object key, HeuristicsNetWritable value, Context context) throws IOException, InterruptedException {

    }
}
