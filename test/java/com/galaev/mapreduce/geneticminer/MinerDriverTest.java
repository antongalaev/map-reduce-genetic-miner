package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.deckfour.xes.info.XLogInfo;
import org.junit.Test;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
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

import java.util.Arrays;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class MinerDriverTest {

    public static final String INPUT_PATH = "population/final/part-00000";

    @Test
    public void testWriteInitialPopulation() throws Exception {
        MinerDriver.writeInitialPopulation();
    }

    @Test
    public void testReadInitialPopulation() throws Exception {
        MinerDriver.readPopulation(MinerDriver.OUTPUT_PATH + "1/part-00000");
    }
    GeneticMinerSettings settings = new GeneticMinerSettings();
    private double[] bestFitness;

    @Test
    public void testGenMiner() throws Exception {
        XLogInfo logInfo = MinerDriver.getLogInfo();
        HeuristicsNet[] population = new HeuristicsNet[settings.getPopulationSize()];
        Fitness fitness = FitnessFactory.getFitness(settings.getFitnessType(), logInfo,
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

        XLogInfo logInfo = MinerDriver.getLogInfo();
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


}