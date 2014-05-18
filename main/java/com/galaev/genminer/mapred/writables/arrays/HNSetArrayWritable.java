package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.processmining.models.heuristics.impl.HNSet;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/05/2014
 * Time: 16:26
 */
public class HNSetArrayWritable extends ArrayWritable {
    public HNSetArrayWritable() {
        super(HNSet.class);
    }
}
