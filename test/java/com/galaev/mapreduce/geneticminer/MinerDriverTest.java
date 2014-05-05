package com.galaev.mapreduce.geneticminer;

import org.junit.Test;

public class MinerDriverTest {

    @Test
    public void testWriteInitialPopulation() throws Exception {
        MinerDriver driver = new MinerDriver();
        driver.writeInitialPopulation();
    }
}