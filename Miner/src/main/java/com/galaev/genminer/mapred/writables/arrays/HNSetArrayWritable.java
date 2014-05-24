package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.processmining.models.heuristics.impl.HNSet;

/**
 * Writable wrapper for HNSet arrays.
 *
 * @author Anton Galaev
 */
public class HNSetArrayWritable extends ArrayWritable {
    public HNSetArrayWritable() {
        super(HNSet.class);
    }
}
