package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 03/05/2014
 * Time: 19:43
 */
public class ByteArrayWritable extends ArrayWritable {
    public ByteArrayWritable() {
        super(ByteWritable.class);
    }
}