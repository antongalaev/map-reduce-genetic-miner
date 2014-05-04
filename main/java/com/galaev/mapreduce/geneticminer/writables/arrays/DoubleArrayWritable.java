package com.galaev.mapreduce.geneticminer.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 03/05/2014
 * Time: 19:40
 */
public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }
}
