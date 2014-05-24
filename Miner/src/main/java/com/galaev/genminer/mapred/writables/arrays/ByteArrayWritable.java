package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;

/**
 * Writable wrapper for byte arrays.
 *
 * @author Anton Galaev
 */
public class ByteArrayWritable extends ArrayWritable {
    public ByteArrayWritable() {
        super(ByteWritable.class);
    }
}