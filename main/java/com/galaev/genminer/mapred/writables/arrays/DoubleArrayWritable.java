package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Writable wrapper for double arrays.
 *
 * @author Anton Galaev
 */
public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }
}
