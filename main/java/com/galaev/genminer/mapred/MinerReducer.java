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
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 04/04/2014
 * Time: 23:59
 */
public class MinerReducer extends MapReduceBase
        implements Reducer<IntWritable, HeuristicsNetImpl, IntWritable, HeuristicsNetImpl> {

    private static final Logger logger = LoggerFactory.getLogger(MinerReducer.class);

    GeneticMinerSettings settings = new GeneticMinerSettings();
    Random generator = new Random(settings.getSeed());

    Crossover crossover = CrossoverFactory.getCrossover(settings.getCrossoverType(), generator);
    Mutation mutation = MutationFactory.getMutation(settings.getMutationType(), generator,
            settings.getMutationRate());
    SelectionMethod selectionMethod = SelectionMethodFactory.getSelectionMethods(settings.getSelectionType(),
            generator);
    BuildPopulation buildNextPopulation = NextPopulationFactory.getPopulation(selectionMethod, generator, settings
            .getCrossoverRate(), settings.getMutationRate(), settings.getElitismRate(), crossover, mutation);

    {
        logger.info("In reducer " + this.toString());
    }

    @Override
    public void reduce(IntWritable key, Iterator<HeuristicsNetImpl> values, OutputCollector<IntWritable, HeuristicsNetImpl> output, Reporter reporter) throws IOException {
        // collect the input into array
        List<HeuristicsNet> netsList = new ArrayList<>();
        while (values.hasNext()) {
           netsList.add(values.next().copy());
        }
        HeuristicsNet[] nets = netsList.toArray(new HeuristicsNet[netsList.size()]);

        // build next population
        HeuristicsNet[] result = buildNextPopulation.build(nets);

        // write it to the output with the right keys
        for (HeuristicsNet heuristicsNet : result) {
            HeuristicsNetImpl net = (HeuristicsNetImpl) heuristicsNet;
            output.collect(new IntWritable(net.getKey()), net);
            //logger.info("Key: " + net.getKey() + " fitness: " + net.getFitness());
        }
        //logger.info("Population part #" + key + " is reduced. Size = " + nets.length);
    }
}
