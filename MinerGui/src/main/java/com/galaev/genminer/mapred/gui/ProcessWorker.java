package com.galaev.genminer.mapred.gui;

import javax.swing.JTextArea;
import javax.swing.SwingWorker;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Worker class for running MapReduce Genetic Miner algorithm.
 * Runs the process in background and publishes console output to the text area.
 *
 * @author Anton Galaev
 */
public class ProcessWorker extends SwingWorker<Object,String> {

    private ProcessBuilder builder;
    private JTextArea textArea;
    private MinerGui mainFrame;

    /**
     * Public constructor.
     *
     * @param builder process,that runs the algorithm
     * @param textArea text area on the frame, to write the output
     * @param mainFrame the frame
     */
    public ProcessWorker(ProcessBuilder builder, JTextArea textArea, MinerGui mainFrame) {
        this.builder = builder;
        this.textArea = textArea;
        this.mainFrame = mainFrame;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     * <p/>
     * <p/>
     * Note that this method is executed only once.
     * <p/>
     * <p/>
     * Note: this method is executed in a background thread.
     *
     * @return the computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    protected Object doInBackground() throws Exception {
        Process p;
        try {
            p = builder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                publish(line + "\n");
            }
            reader.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * Prints console output to the text area.
     *
     * @param chunks console output parts
     */
    @Override
    protected void process(List<String> chunks) {
        StringBuilder buffer = new StringBuilder();
        for (String chunk : chunks) {
            buffer.append(chunk);
        }
        textArea.append(buffer.toString());
    }

    /**
     * Show results on the frame,
     * after algorithm is done.
     */
    @Override
    protected void done() {
        mainFrame.showResults();
    }
}
