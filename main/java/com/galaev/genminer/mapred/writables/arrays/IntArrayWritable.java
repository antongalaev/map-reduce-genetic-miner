package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Writable wrapper for int arrays.
 *
 * @author Anton Galaev
 */
public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable()  {
         super(IntWritable.class);
    }

}
