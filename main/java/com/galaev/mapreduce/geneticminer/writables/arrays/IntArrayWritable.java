package com.galaev.mapreduce.geneticminer.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 03/05/2014
 * Time: 17:09
 */
public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable()  {
         super(IntWritable.class);
    }

}
