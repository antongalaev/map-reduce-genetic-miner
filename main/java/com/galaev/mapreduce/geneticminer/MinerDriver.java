package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.InitialPopulationFactory;

import java.io.File;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:59
 */
public class MinerDriver {

    public static void main(String[] args) throws Exception {



        Job job = new Job();
        job.setJarByClass(MinerDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        job.setMapperClass(MinerMapper.class);
        job.setReducerClass(MinerReducer.class);

        job.setOutputKeyClass(HeuristicsNet.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



    public void writeInitialPopulation() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("population/initial");


        XesXmlParser parser = new XesXmlParser();
        List<XLog> logs = parser.parse(new File("logs/example1.xes"));
        GeneticMinerSettings settings = new GeneticMinerSettings();
        XLog log = logs.get(0);
        XLogInfo logInfo = XLogInfoFactory.createLogInfo(log);
        Random generator = new Random(settings.getSeed());
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator,
                logInfo, settings.getPower()).build(population);

        LongWritable key = new LongWritable();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,path,LongWritable.class, HeuristicsNetImpl.class);
        for (int i = 0; i < population.length; ++i) {
            key.set(i);
            writer.append(key, population[i]);
        }
    }
}
