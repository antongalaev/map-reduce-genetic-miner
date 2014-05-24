package com.galaev.genminer.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;
import org.junit.BeforeClass;
import org.junit.Test;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.array.visualization.HeuristicsMinerVisualizationPanel;
import org.processmining.plugins.heuristicsnet.miner.genetic.fitness.Fitness;
import org.processmining.plugins.heuristicsnet.miner.genetic.fitness.FitnessFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.Crossover;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.CrossoverFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.Mutation;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.MutationFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.BuildPopulation;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.InitialPopulationFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.NextPopulationFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.selection.SelectionMethod;
import org.processmining.plugins.heuristicsnet.miner.genetic.selection.SelectionMethodFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.util.MethodsOverHeuristicsNets;

import javax.swing.*;
import java.io.File;
import java.io.InputStream;
import java.util.*;

import static junit.framework.Assert.assertEquals;


/**
 * Some tests for {@code MinerDriver} class.
 *
 * @see com.galaev.genminer.mapred.MinerDriver
 * @author Anton Galaev
 */
public class MinerDriverTest {

    public static final String INPUT_PATH = "population/final/part-00000";
    private static XLogInfo logInfo;


    @BeforeClass
    public static void setUp() throws Exception {
        XesXmlParser parser = new XesXmlParser();
        //List<XLog> logs = parser.parse(new File("/Users/anton/Downloads/Chapter_8/reviewing.xes"));
        List<XLog> logs = parser.parse(new File("/Users/anton/Dropbox/Coursework/logs/example-logs/exercise5.xes"));
        XLog log = logs.get(0);
        logInfo = XLogInfoFactory.createLogInfo(log);
    }


    @Test
    public void visualizeResult() throws Exception {
        List<HeuristicsNetImpl> result = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            // readPopulation(MinerDriver.POPULATIONS_PATH + i + "/part-00000");
            result.addAll(Arrays.asList(readPopulation("/Users/anton/gen3/part-0000" + i)));
        }
        final HeuristicsMinerVisualizationPanel panel = new HeuristicsMinerVisualizationPanel(null,
                result.toArray(new HeuristicsNet[result.size()]));

                new JFrame() {{
                    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
                    setTitle("Heuristics Net Visualization");
                    setSize(400, 300);
                    add(panel);
                }}.setVisible(true);
        Thread.sleep(120000);
    }

    public static HeuristicsNetImpl[] readPopulation(String input) throws Exception {
        System.out.println("Reading : " + input);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(input);


        List<HeuristicsNetImpl> result = new ArrayList<>();

        IntWritable key = new IntWritable();
        HeuristicsNetImpl net = new HeuristicsNetImpl();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            while (reader.next(key, net)) {
                result.add(net);
                net = new HeuristicsNetImpl();
            }

        } finally {
            IOUtils.closeStream(reader);
        }

        Fitness fitness = FitnessFactory.getFitness(3, logInfo, FitnessFactory.ALL_FITNESS_PARAMETERS);
        HeuristicsNetImpl[] population = (HeuristicsNetImpl[]) fitness.calculate(result.toArray(new HeuristicsNetImpl[result.size()]));
        System.out.println("\n Continuous new " + Collections.min(Arrays.asList(population)).getFitness() +
                " - " + Collections.max(Arrays.asList(population)).getFitness());
        printFitness(population);
//
//        fitness = FitnessFactory.getFitness(4, logInfo, FitnessFactory.ALL_FITNESS_PARAMETERS);
//        population = (HeuristicsNetImpl[]) fitness.calculate(population);
//        System.out.println("\n Punishment new "  + Collections.min(Arrays.asList(population)).getFitness() +
//                " - "  + Collections.max(Arrays.asList(population)).getFitness());
//        //printFitness(population);

        return population;
    }

    public static void printFitness(HeuristicsNetImpl[] population) {
        for (HeuristicsNetImpl aPopulation : population) {
            System.out.print(aPopulation.getFitness() + "---");
        }
    }


    @Test
    public void testReadResults() throws Exception {
        //for (int i = 0; i < 4; i++) {
           // readPopulation(MinerDriver.POPULATIONS_PATH + i + "/part-00000");
            readPopulation("/Users/anton/Documents/result_at_1400708966783");
        //}
    }



    GeneticMinerSettings settings = new GeneticMinerSettings();
    private double[] bestFitness;

    @Test
    public void testGenMiner() throws Exception {
        long start = System.currentTimeMillis();
        settings.setPopulationSize(10000);
        settings.setMaxGeneration(5);

        XesXmlParser parser = new XesXmlParser();
        List<XLog> logs = parser.parse(new File("/Users/anton/Dropbox/Coursework/logs/Chapter_8/reviewing.xes"));
        XLog log = logs.get(0);
        logInfo = XLogInfoFactory.createLogInfo(log);

        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        Fitness fitness = FitnessFactory.getFitness(3, logInfo,
                FitnessFactory.ALL_FITNESS_PARAMETERS);
        Random generator = new Random(settings.getSeed());
        int populationNumber = 0;

        bestFitness = new double[settings.getMaxGeneration()];

        //building the initial population
        population = InitialPopulationFactory.getPopulation(settings.getInitialPopulationType(), generator, logInfo,
                settings.getPower()).build(population);
        population = fitness.calculate(population);
        updateStatistics(populationNumber, population);
        populationNumber++;

        //building the next generations
        SelectionMethod selectionMethod = SelectionMethodFactory.getSelectionMethods(settings.getSelectionType(),
                generator);

        Crossover crossover = CrossoverFactory.getCrossover(settings.getCrossoverType(), generator);
        Mutation mutation = MutationFactory.getMutation(settings.getMutationType(), generator, settings
                .getMutationRate());
        BuildPopulation buildNextPopulation = NextPopulationFactory.getPopulation(selectionMethod, generator, settings
                .getCrossoverRate(), settings.getMutationRate(), settings.getElitismRate(), crossover, mutation);

        double bf = 0;

        for (; (populationNumber < settings.getMaxGeneration()); populationNumber++) {
            population = buildNextPopulation.build(population);
            population = fitness.calculate(population);
            updateStatistics(populationNumber, population);
            bf = bestFitness[populationNumber];
        }

        //Cleaning the HeuristicsNets, and increasingly ordering them in the population based on their fitness measures
        population = MethodsOverHeuristicsNets.removeUnusedElements(population, fitness);
        Arrays.sort(population);

        System.out.println("N = " + populationNumber + " popSize = " + population.length);
        System.out.println(Arrays.toString(bestFitness));
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Elapsed: " + elapsed / 1000);
    }

    private void updateStatistics(int populationNumber, HeuristicsNet[] population) {
        if (settings.isKeepStatistics()) {
            Arrays.sort(population);
            bestFitness[populationNumber] = population[population.length - 1].getFitness();
        }
    }

    @Test
    public void testReadAndWrite() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(INPUT_PATH);

        XLogInfo logInfo = getLogInfo();
        GeneticMinerSettings settings = new GeneticMinerSettings();
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



        HeuristicsNetImpl net = new HeuristicsNetImpl();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            int i = 0;
            while (reader.next(net, value)) {
                assertEquals(population[i++], net);
                System.out.println(net.toString() + " -------- "
                        + value.toString());
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    /**
     * Copied from {@code MinerDriver} class.
     * @see com.galaev.genminer.mapred.MinerDriver
     * @return log info
     * @throws Exception
     */
    private XLogInfo getLogInfo() throws Exception {
        Path path = new Path("hdfs://localhost:9000/user/anton/log.xes");
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream in = null;
        try {
            XesXmlParser parser = new XesXmlParser();
            in = fs.open(path);
            List<XLog> logs = parser.parse(in);
            XLog log = logs.get(0);
            return XLogInfoFactory.createLogInfo(log);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}