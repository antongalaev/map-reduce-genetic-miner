package com.galaev.genminer.mapred.writables.arrays;

import org.processmining.models.heuristics.impl.HNSubSet;
import org.apache.hadoop.io.ArrayWritable;

/**
 * Writable wrapper for HNSubSet arrays.
 *
 * @author Anton Galaev
 */
public class HNSubSetArrayWritable extends ArrayWritable {

    public HNSubSetArrayWritable() {
        super(HNSubSet.class);
    }
}
