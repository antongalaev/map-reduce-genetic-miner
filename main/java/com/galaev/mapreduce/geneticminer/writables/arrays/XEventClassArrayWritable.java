package com.galaev.mapreduce.geneticminer.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.deckfour.xes.classification.XEventClassWritable;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/05/2014
 * Time: 14:04
 */
public class XEventClassArrayWritable extends ArrayWritable {
    public XEventClassArrayWritable() {
        super(XEventClassWritable.class);
    }
}