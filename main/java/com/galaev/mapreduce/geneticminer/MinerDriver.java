package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.*;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.miner.genetic.fitness.Fitness;
import org.processmining.plugins.heuristicsnet.miner.genetic.fitness.FitnessFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.InitialPopulationFactory;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:59
 */
public class MinerDriver {

    public static final String INPUT_PATH = "population/initial";
    public static final String OUTPUT_PATH = "population/gen";
    public static final int NUM_GENERATIONS = 10;

    public static void main(String[] args) throws Exception {
        writeInitialPopulation();

        for (int i = 0; i < NUM_GENERATIONS; i++) {
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
        conf.setOutputKeyClass(HeuristicsNetImpl.class);
        conf.setOutputValueClass(DoubleWritable.class);
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
        settings.setPopulationSize(100);
        Random generator = new Random(settings.getSeed());
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator,
                logInfo, settings.getPower()).build(population);

        DoubleWritable value = new DoubleWritable(0);
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs,conf,path, HeuristicsNetImpl.class, DoubleWritable.class);
            for (HeuristicsNet individual : population) {
                writer.append(individual, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }

    }



    public static void readPopulation(String input) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(input);


        List<HeuristicsNetImpl> result = new ArrayList<>();

        DoubleWritable value = new DoubleWritable(0);
        HeuristicsNetImpl net = new HeuristicsNetImpl();
        HeuristicsNetImpl prev = new HeuristicsNetImpl();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            while (reader.next(net, value)) {
                result.add(net);
                System.out.print(net.getFitness() == prev.getFitness() ? "1" : "0");
                prev = net;
                net = new HeuristicsNetImpl();
            }

        } finally {
            IOUtils.closeStream(reader);
        }

        System.out.println("\n Initial :" + Collections.min(result).getFitness() + " ----" + result.size() + "---- "
                + Collections.max(result).getFitness());

        XLogInfo logInfo = getLogInfo();
        Fitness fitness = FitnessFactory.getFitness(3, logInfo, FitnessFactory.ALL_FITNESS_PARAMETERS);
        HeuristicsNetImpl[] population = (HeuristicsNetImpl[]) fitness.calculate(result.toArray(new HeuristicsNetImpl[result.size()]));
        System.out.println("\n Continuous new " + Collections.min(Arrays.asList(population)).getFitness() + " ----" + result.size() + "---- "
                + Collections.max(Arrays.asList(population)).getFitness());
        printFitness(population);


        for (int i = 0; i < population.length; i++) {
            SingleFitness singleFitness = new SingleFitness(logInfo);
            population[i] = (HeuristicsNetImpl) singleFitness.calculate(population[i]);
        }
        System.out.println("\n Single Continuous new " + Collections.min(Arrays.asList(population)).getFitness() + " ----" + result.size() + "---- "
                + Collections.max(Arrays.asList(population)).getFitness());
        printFitness(population);

        fitness = FitnessFactory.getFitness(4, logInfo, FitnessFactory.ALL_FITNESS_PARAMETERS);
        population = (HeuristicsNetImpl[]) fitness.calculate(population);
        System.out.println("\n Punishment new " + Collections.min(Arrays.asList(population)).getFitness() + " ----" + result.size() + "---- "
                + Collections.max(Arrays.asList(population)).getFitness());
        printFitness(population);
    }

    private static void printFitness(HeuristicsNetImpl[] population) {
        for (HeuristicsNetImpl aPopulation : population) {
            System.out.println(aPopulation.getFitness());
        }
    }
}
