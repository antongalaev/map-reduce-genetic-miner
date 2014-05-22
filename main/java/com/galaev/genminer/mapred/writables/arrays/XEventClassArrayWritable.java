package com.galaev.genminer.mapred.writables.arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.deckfour.xes.classification.XEventClass;

/**
 * Writable wrapper for XEventClass arrays.
 *
 * @author Anton Galaev
 */
public class XEventClassArrayWritable extends ArrayWritable {
    public XEventClassArrayWritable() {
        super(XEventClass.class);
    }
}
