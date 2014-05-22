package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Writable wrapper for String arrays.
 *
 * @author Anton Galaev
 */
public class StringArrayWritable extends ArrayWritable {
    public StringArrayWritable() {
        super(Text.class);
    }
}
