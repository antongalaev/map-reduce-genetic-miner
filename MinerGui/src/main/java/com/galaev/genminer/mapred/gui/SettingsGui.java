package com.galaev.genminer.mapred.gui;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

/**
 * This class represents the frame,
 * that is intended to input all settings,
 * that are necessary to run MapReduce Genetic Miner Algorithm.
 *
 * @author Anton Galaev
 */
public class SettingsGui extends JFrame {

    // executable jar path controls
    private JPanel jarPanel = new JPanel();
    private JLabel jarLabel = new JLabel("Executable Jar:");
    private JTextField jarField = new JTextField();
    private JButton jarButton = new JButton("Choose");
    // input path controls
    private JPanel inputPanel = new JPanel();
    private JLabel inputLabel = new JLabel("Input Path:      ");
    private JTextField inputField = new JTextField();
    private JButton inputButton = new JButton("Choose");
    // output path controls
    private JPanel outputPanel = new JPanel();
    private JLabel outputLabel = new JLabel("Output Path:   ");
    private JTextField outputField = new JTextField();
    private JButton outputButton = new JButton("Choose");
    // controls for number of generations and population size
    private JPanel populationPanel = new JPanel();
    private JPanel generationsPanel = new JPanel();
    private JLabel populationLabel = new JLabel("Population size:                 ");
    private JLabel generationsLabel = new JLabel("Number of generations:      ");
    private JTextField populationField = new JTextField();
    private JTextField generationsField = new JTextField();

    private JButton okButton = new JButton("OK");

    private MinerGui parentFrame;
    private Settings settings;


    public SettingsGui(MinerGui parent) {
        super();
        setTitle("Settings");
        setSize(400, 300);
        Toolkit toolkit = Toolkit.getDefaultToolkit();
        int screenWidth = (int) toolkit.getScreenSize().getWidth();
        int screenHeight = (int) toolkit.getScreenSize().getHeight();
        setLocation((screenWidth - getWidth()) / 2, (screenHeight - getHeight()) / 2);
        setLayout(new FlowLayout(FlowLayout.LEFT));

        jarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                int retVal = chooser.showOpenDialog(SettingsGui.this);
                if (retVal == JFileChooser.APPROVE_OPTION) {
                    File file = chooser.getSelectedFile();
                    jarField.setText(file.getAbsolutePath());
                }
            }
        });
        inputButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                int retVal = chooser.showOpenDialog(SettingsGui.this);
                if (retVal == JFileChooser.APPROVE_OPTION) {
                    File file = chooser.getSelectedFile();
                    inputField.setText(file.getAbsolutePath());
                }
            }
        });
        outputButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
                int retVal = chooser.showOpenDialog(SettingsGui.this);
                if (retVal == JFileChooser.APPROVE_OPTION) {
                    File file = chooser.getSelectedFile();
                    outputField.setText(file.getAbsolutePath());
                }
            }
        });
        okButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String jarPath = jarField.getText();
                File file = new File(jarPath);
                if (! (file.exists() && file.isFile())) {
                    showMessage("Jar file does not exist");
                    parentFrame.setConfigured(false);
                    return;
                }
                String inputPath = inputField.getText();
                file = new File(inputPath);
                if (! (file.exists() && file.isFile())) {
                    showMessage("Input file does not exist");
                    parentFrame.setConfigured(false);
                    return;
                }
                String outputPath = outputField.getText();
                file = new File(outputPath);
                if (! (file.exists() && file.isDirectory())) {
                    showMessage("Output path does not exist");
                    parentFrame.setConfigured(false);
                    return;
                }
                int numGenerations;
                int populationSize;
                try {
                    numGenerations = Integer.parseInt(generationsField.getText());
                    populationSize = Integer.parseInt(populationField.getText());
                } catch (NumberFormatException nfe) {
                    showMessage("Number of generations or population size are not correct");
                    parentFrame.setConfigured(false);
                    return;
                }
                if (populationSize < 600) {
                    showMessage("Population size should be at least 600");
                    parentFrame.setConfigured(false);
                    return;
                }
                if (numGenerations < 1) {
                    showMessage("Number of generations should be at least 1");
                    parentFrame.setConfigured(false);
                    return;
                }
                settings = new Settings(jarPath, inputPath, outputPath, populationSize, numGenerations);
                parentFrame.setSettings(settings);
                parentFrame.setConfigured(true);
                SettingsGui.this.setVisible(false);
            }
        });

        jarField.setEditable(false);
        inputField.setEditable(false);
        outputField.setEditable(false);
        jarField.setColumns(15);
        inputField.setColumns(15);
        outputField.setColumns(15);
        populationField.setColumns(15);
        generationsField.setColumns(15);

        // set values if settings already exist
        parentFrame = parent;
        if (parentFrame.getSettings() != null) {
            Settings st = parentFrame.getSettings();
            jarField.setText(st.getJarPath());
            inputField.setText(st.getInputPath());
            outputField.setText(st.getOutputPath());
            populationField.setText("" + st.getPopulationSize());
            generationsField.setText("" + st.getNumGenerations());
        }

        // add jar input controls
        jarPanel.add(jarLabel);
        jarPanel.add(jarField);
        jarPanel.add(jarButton);
        add(jarPanel);
        // add input controls
        inputPanel.add(inputLabel);
        inputPanel.add(inputField);
        inputPanel.add(inputButton);
        add(inputPanel);
        // add output controls
        outputPanel.add(outputLabel);
        outputPanel.add(outputField);
        outputPanel.add(outputButton);
        add(outputPanel);
        add(new JSeparator());
        // add controls for population size
        populationPanel.add(populationLabel);
        populationPanel.add(populationField);
        add(populationPanel);
        // add controls for number of generations
        generationsPanel.add(generationsLabel);
        generationsPanel.add(generationsField);
        add(generationsPanel);
        // set alignment
        jarPanel.setAlignmentX(LEFT_ALIGNMENT);
        inputPanel.setAlignmentX(LEFT_ALIGNMENT);
        outputPanel.setAlignmentX(LEFT_ALIGNMENT);
        populationPanel.setAlignmentX(LEFT_ALIGNMENT);
        generationsPanel.setAlignmentX(LEFT_ALIGNMENT);
        // add button
        add(okButton);
        setVisible(true);
    }

    private void showMessage(String message) {
        JOptionPane.showMessageDialog(this, message);
    }
}
