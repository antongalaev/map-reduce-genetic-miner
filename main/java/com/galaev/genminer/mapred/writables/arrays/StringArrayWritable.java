package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/05/2014
 * Time: 02:37
 */
public class StringArrayWritable extends ArrayWritable {
    public StringArrayWritable() {
        super(Text.class);
    }
}
