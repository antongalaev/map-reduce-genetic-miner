package com.galaev.genminer.mapred.gui;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.processmining.models.heuristics.HeuristicsNet;
import org.processmining.models.heuristics.impl.HeuristicsNetImpl;
import org.processmining.plugins.heuristicsnet.array.visualization.HeuristicsMinerVisualizationPanel;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import java.awt.BorderLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This class represents a frame,
 * that provides GUI for MapReduce Genetic Miner algorithm.
 *
 * @author Anton Galaev
 */
public class MinerGui extends JFrame {

    /**
     * Constructor for the frame.
     */
    public MinerGui() {
        super();
        // window settings
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setTitle("MapReduce Genetic Miner Gui");
        setSize(1000, 600);
        Toolkit toolkit = Toolkit.getDefaultToolkit();
        int screenWidth = (int) toolkit.getScreenSize().getWidth();
        int screenHeight = (int) toolkit.getScreenSize().getHeight();
        setLocation((screenWidth - getWidth()) / 2, (screenHeight - getHeight()) / 2);
        // settings button
        final JButton settingsButton = new JButton("Configure");
        settingsButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                SettingsGui settings = new SettingsGui(MinerGui.this);
            }
        });
        // run button
        runButton = new JButton("Run");
        runButton.setEnabled(false);
        runButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                settings.setStartTime(System.currentTimeMillis());
                ProcessBuilder builder = new ProcessBuilder("hadoop", "jar",
                        settings.getJarPath(),
                        "com.galaev.genminer.mapred.MinerDriver", settings.getInputPath(), settings.getOutputPath(),
                        "" + settings.getPopulationSize(), "" + settings.getNumGenerations(),
                        "" + settings.getStartTime());
                builder.redirectErrorStream(true);
                textArea.append("Algorithm process started...\n");
                runButton.setEnabled(false);
                ProcessWorker worker = new ProcessWorker(builder, textArea, MinerGui.this);
                worker.execute();
            }
        });
        // text area
        textArea.setRows(11);
        JScrollPane scrollPane = new JScrollPane(textArea);
        // add everything to the frame
        add(scrollPane, BorderLayout.PAGE_START);
        JMenuBar bar = new JMenuBar();
        bar.add(settingsButton);
        bar.add(runButton);
        setJMenuBar(bar);
        setVisible(true);
    }

    /**
     * Shows the results of the algorithm on the frame, after it's done.
     *
     * @see com.galaev.genminer.mapred.gui.ProcessWorker
     */
    void showResults() {
        // get the file system object
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // create path for reading
        Path path = new Path(settings.getOutputPath() + "/result_at_" + settings.getStartTime());

        // read all the results from sequence file to the list
        java.util.List<HeuristicsNetImpl> results = new ArrayList<>();
        IntWritable key = new IntWritable();
        HeuristicsNetImpl net = new HeuristicsNetImpl();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            while (reader.next(key, net)) {
                results.add(net);
                net = new HeuristicsNetImpl();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
        // create new visualizer panel
        if (visualizerPanel != null) {
            remove(visualizerPanel);
        }
        visualizerPanel = new HeuristicsMinerVisualizationPanel(null,
                results.toArray(new HeuristicsNet[results.size()]));
        add(visualizerPanel, BorderLayout.CENTER);
        runButton.setEnabled(true);
        // update the frame
        revalidate();
       // repaint();
    }

    /**
     * Main method.
     * Sets look and feel and creates the frame.
     *
     * @param args no args
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                try {
                    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    new MinerGui();
                } catch (Exception e) {
                    System.out.println("A mistake was made. Try once more.");
                }
            }
        });
    }

    // the "Run" button
    private JButton runButton;
    // Text area, where all the output goes
    private JTextArea textArea = new JTextArea();
    // The panel where all the visualization goes
    private JPanel visualizerPanel;
    // algorithm settings
    private Settings settings;
    // shows whether algorithm is configured (depends on settings)
    private boolean configured;

    /**
     * Getter for {@code configured} variable.
     * If it is true, we can run the algorithm.
     *
     * @return whether algorithm is configured and all settings are here
     */
    public boolean isConfigured() {
        return configured;
    }

    /**
     * Setter for  {@code configured} variable.
     * If it becomes true, we can enable the "Run" button and run the algorithm.
     *
     * @param configured new value
     */
    public void setConfigured(boolean configured) {
        this.configured = configured;
        if (configured) {
            runButton.setEnabled(true);
        } else {
            runButton.setEnabled(false);
        }
    }

    /**
     * Getter for algorithm settings
     *
     * @return algorithm settings
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Setter for algorithm settings
     *
     * @param settings algorithm settings
     */
    public void setSettings(Settings settings) {
        this.settings = settings;
    }
}
