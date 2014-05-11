package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.*;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.InitialPopulationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(MinerDriver.class);

    public static final String INPUT_PATH = "population/initial";
    public static final String OUTPUT_PATH = "population/gen";
    public static final int NUM_GENERATIONS = 10;

    public static void main(String[] args) throws Exception {
        writeInitialPopulation();

        for (int i = 0; i < NUM_GENERATIONS; i++) {
            logger.info("--------------------GENERATION #" + i);
            JobConf nextJob = createJob(i);
            JobClient.runJob(nextJob);
        }
    }

    private static JobConf createJob(int i) {
        JobConf conf = new JobConf(MinerDriver.class);
        conf.setJobName("Mining gen #" + i);

        conf.set("dfs.block.size", "1048576");
        conf.setNumReduceTasks(4);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        if (i == 0) {
            FileInputFormat.addInputPath(conf, new Path(INPUT_PATH));
        } else {
            FileInputFormat.addInputPath(conf, new Path(OUTPUT_PATH + (i - 1)));
        }
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH + i));

        conf.setPartitionerClass(MinerPartitioner.class);
        conf.setMapperClass(MinerMapper.class);
        conf.setReducerClass(MinerReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(HeuristicsNetImpl.class);
        return conf;
    }


    public static XLogInfo getLogInfo() throws Exception {
        XesXmlParser parser = new XesXmlParser();
        List<XLog> logs = parser.parse(new File("logs/example1.xes"));
        XLog log = logs.get(0);
        return XLogInfoFactory.createLogInfo(log);
    }

    public static void writeInitialPopulation() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(INPUT_PATH);

        XLogInfo logInfo = getLogInfo();
        GeneticMinerSettings settings = new GeneticMinerSettings();
        settings.setPopulationSize(10000);
        Random generator = new Random(settings.getSeed());
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator,
                logInfo, settings.getPower()).build(population);

        IntWritable key = new IntWritable();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, HeuristicsNetImpl.class);
            for (int i = 0; i < population.length; ++i) {
                key.set(i);
                ((HeuristicsNetImpl) population[i]).setKey(i);
                writer.append(key, population[i]);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }


}
