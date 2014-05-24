package com.galaev.genminer.mapred.gui;

/**
 * This class holds all settings,
 * required for MapReduce Genetic Miner Algorithm.
 *
 * @author Anton Galaev
 */
public class Settings {

    /** input local path to the log */
    private String inputPath;
    /** output local path to results */
    private String outputPath;
    /** path to the executable jar*/
    private String jarPath;
    /** size of the population */
    private int populationSize;
    /** number of generations */
    private int numGenerations;
    /** start of the algorithm */
    private long startTime;

    /**
     * Public constructor for settings class.
     *
     * @param jarPath path to the executable jar
     * @param inputPath input local path to the log
     * @param outputPath output local path to results
     * @param populationSize size of the population
     * @param numGenerations number of generations
     */
    public Settings(String jarPath, String inputPath, String outputPath, int populationSize, int numGenerations) {
        this.jarPath = jarPath;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.populationSize = populationSize;
        this.numGenerations = numGenerations;
    }

    // Getters and setters

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public int getPopulationSize() {
        return populationSize;
    }

    public void setPopulationSize(int populationSize) {
        this.populationSize = populationSize;
    }

    public int getNumGenerations() {
        return numGenerations;
    }

    public void setNumGenerations(int numGenerations) {
        this.numGenerations = numGenerations;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }
}
