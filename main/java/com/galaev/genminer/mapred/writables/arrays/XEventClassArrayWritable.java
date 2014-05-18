package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.deckfour.xes.classification.XEventClass;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/05/2014
 * Time: 14:04
 */
public class XEventClassArrayWritable extends ArrayWritable {
    public XEventClassArrayWritable() {
        super(XEventClass.class);
    }
}
