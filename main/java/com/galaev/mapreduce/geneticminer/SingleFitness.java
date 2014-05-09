package com.galaev.mapreduce.geneticminer;

import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.model.XTrace;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.ContinuousSemanticsParser;

import java.util.Iterator;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: anton
 * Date: 06/05/2014
 * Time: 17:24
 */
public class SingleFitness {
    private XLogInfo logInfo = null;
    private HeuristicsNet individual = null;
    private ContinuousSemanticsParser parser = null;

    private double numPIsWithMissingTokens; //PI = process instance
    private double numMissingTokens; //PI = process instance
    private double numPIsWithExtraTokensLeftBehind;
    private double numExtraTokensLeftBehind;
    private double numParsedWMEs;

    private Random generator = null;

    /**
     * Constructs a new improved continuous semantics fitness for the given log.
     * All fitness values calculated by this object for populations of
     * <code>HeuristicsNet</code> will be based on this log.
     *
     * @param logInfo
     *            information about the log
     */
    public SingleFitness(XLogInfo logInfo) {
        generator = new Random(Long.MAX_VALUE);
        this.logInfo = logInfo;
    }

    /**
     * Calculates the improved continuous semantics fitness of every
     * <code>HeuristicsNet</code> in the individual
     *
     * @param individual
     *            array containing the <code>HeuristicsNet</code> for which a
     *            fitness value will be calculated
     */
    public HeuristicsNet calculate(HeuristicsNet individual) {

        this.individual = individual;
        createParser();
        resetDuplicatesActualFiringAndArcUsage();
        calculatePartialFitness();

        return assignFitness();

    }

    private void resetDuplicatesActualFiringAndArcUsage() {
            individual.resetActivitiesActualFiring();
            individual.resetArcUsage();
    }

    private void createParser() {
        //creating a parser for every individual
        parser = new ContinuousSemanticsParser(individual, generator);
    }

    private void calculatePartialFitness() {

        XTrace pi;
        int numSimilarPIs;
        int numMissingTokens;
        int numExtraTokensLeftBehind;

        Iterator<XTrace> logReaderInstanceIterator = logInfo.getLog().iterator();
        while (logReaderInstanceIterator.hasNext()) {
            pi = logReaderInstanceIterator.next();

            numSimilarPIs = 1;
            parser.parse(pi);
            //partial assignment to variables
            numMissingTokens = parser.getNumMissingTokens();
            if (numMissingTokens > 0) {
                numPIsWithMissingTokens += numSimilarPIs;
                this.numMissingTokens += (numMissingTokens * numSimilarPIs);
            }

            numExtraTokensLeftBehind = parser.getNumExtraTokensLeftBehind();
            if (numExtraTokensLeftBehind > 0) {
                numPIsWithExtraTokensLeftBehind += numSimilarPIs;
                this.numExtraTokensLeftBehind += (numExtraTokensLeftBehind * numSimilarPIs);
            }
            numParsedWMEs += (parser.getNumParsedElements() * numSimilarPIs);
        }
    }

    private HeuristicsNet assignFitness() {

        double fitness = 0;
        double numATEsAtLog = 0;
        double numPIsAtLog = 0;
        double missingTokensDenominator = 0.001;
        double unusedTokensDenominator = 0.001;

        numATEsAtLog = logInfo.getNumberOfEvents();
        numPIsAtLog = logInfo.getNumberOfTraces();

        missingTokensDenominator = numPIsAtLog - numPIsWithMissingTokens + 1;

        unusedTokensDenominator = numPIsAtLog - numPIsWithExtraTokensLeftBehind + 1;

        fitness = (numParsedWMEs - ((numMissingTokens / missingTokensDenominator) + (numExtraTokensLeftBehind / unusedTokensDenominator)))
                / numATEsAtLog;

        individual.setFitness(fitness);

        return individual;
    }
}
