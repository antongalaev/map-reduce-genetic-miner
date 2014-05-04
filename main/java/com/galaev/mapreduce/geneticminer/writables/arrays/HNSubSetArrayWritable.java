package com.galaev.mapreduce.geneticminer.writables.arrays;

import org.processmining.models.heuristics.impl.HNSubSetWritable;
import org.apache.hadoop.io.ArrayWritable;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 03/05/2014
 * Time: 17:25
 */
public class HNSubSetArrayWritable extends ArrayWritable {

    HNSubSetArrayWritable() {
        super(HNSubSetWritable.class);
    }
}
