package com.galaev.mapreduce.geneticminer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.Crossover;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.CrossoverFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.Mutation;
import org.processmining.plugins.heuristicsnet.miner.genetic.geneticoperations.MutationFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.miner.settings.GeneticMinerSettings;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.BuildPopulation;
import org.processmining.plugins.heuristicsnet.miner.genetic.population.NextPopulationFactory;
import org.processmining.plugins.heuristicsnet.miner.genetic.selection.SelectionMethod;
import org.processmining.plugins.heuristicsnet.miner.genetic.selection.SelectionMethodFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:59
 */
public class MinerReducer extends MapReduceBase implements Reducer<HeuristicsNetImpl, DoubleWritable, HeuristicsNetImpl, DoubleWritable> {

    Logger logger = LoggerFactory.getLogger(MinerReducer.class);

    GeneticMinerSettings settings = new GeneticMinerSettings();
    Random generator = new Random(settings.getSeed());

    Crossover crossover = CrossoverFactory.getCrossover(settings.getCrossoverType(), generator);
    Mutation mutation = MutationFactory.getMutation(settings.getMutationType(), generator,
            settings.getMutationRate());
    SelectionMethod selectionMethod = SelectionMethodFactory.getSelectionMethods(settings.getSelectionType(),
            generator);
    BuildPopulation buildNextPopulation = NextPopulationFactory.getPopulation(selectionMethod, generator, settings
            .getCrossoverRate(), settings.getMutationRate(), settings.getElitismRate(), crossover, mutation);

    HeuristicsNetImpl[] window = new HeuristicsNetImpl[10];

    int counter = 0;

    {
        logger.info("In reducer " + this.toString());
    }
    @Override
    public void reduce(HeuristicsNetImpl key, Iterator<DoubleWritable> values, OutputCollector<HeuristicsNetImpl, DoubleWritable> output, Reporter reporter) throws IOException {

        if (counter < window.length) {
            window[counter] = key;
            ++ counter;
        } else {
            counter = 0;
            HeuristicsNet[] result = buildNextPopulation.build(window);
            for (HeuristicsNet individual : result) {
                output.collect((HeuristicsNetImpl) individual, new DoubleWritable(individual.getFitness()));
            }
        }
    }
}
