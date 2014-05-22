package com.galaev.genminer.mapred;

import org.apache.hadoop.io.IntWritable;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Reducer class for MapReduce Genetic Miner algorithm.
 * Creates a new generation from the bunch of individuals, that
 * come inside.
 *
 * @see com.galaev.genminer.mapred.MinerDriver
 * @author Anton Galaev
 */
public class MinerReducer extends MapReduceBase
        implements Reducer<IntWritable, HeuristicsNetImpl, IntWritable, HeuristicsNetImpl> {

    private static final Logger logger = LoggerFactory.getLogger(MinerReducer.class);

    // settings and other necessary tools for creating the population
    GeneticMinerSettings settings = new GeneticMinerSettings();
    Random generator = new Random(settings.getSeed());
    Crossover crossover = CrossoverFactory.getCrossover(settings.getCrossoverType(), generator);
    Mutation mutation = MutationFactory.getMutation(settings.getMutationType(), generator,
            settings.getMutationRate());
    SelectionMethod selectionMethod = SelectionMethodFactory.getSelectionMethods(settings.getSelectionType(),
            generator);
    BuildPopulation buildNextPopulation = NextPopulationFactory.getPopulation(selectionMethod, generator, settings
            .getCrossoverRate(), settings.getMutationRate(), settings.getElitismRate(), crossover, mutation);

    /**
     * Reduce method.
     * Reads input values into array. Performs genetic operations
     * over that array(selection, crossover and mutation). Thus, creates a new generation.
     *
     * @param key number of the split
     * @param values all nets in the split
     * @param output collector
     * @param reporter reporter
     * @throws IOException
     */
    @Override
    public void reduce(IntWritable key, Iterator<HeuristicsNetImpl> values, OutputCollector<IntWritable, HeuristicsNetImpl> output, Reporter reporter) throws IOException {
        // collect the input values into array
        List<HeuristicsNet> netsList = new ArrayList<>();
        while (values.hasNext()) {
           netsList.add(values.next().copy());
        }
        HeuristicsNet[] nets = netsList.toArray(new HeuristicsNet[netsList.size()]);
        // build next population
        HeuristicsNet[] next = buildNextPopulation.build(nets);

        // write it to the output with original keys
        for (HeuristicsNet heuristicsNet : next) {
            HeuristicsNetImpl net = (HeuristicsNetImpl) heuristicsNet;
            output.collect(new IntWritable(net.getKey()), net);
        }
    }
}
