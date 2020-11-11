package com.ruben.javaDataProcessing;

import static org.junit.Assert.*;

import org.junit.Test;
import com.opencsv.*; 

/**
 * Unit test for simple App.
 */
public class CSVUtilsTest 
{

    private final String LINE = "1,renfe,MADRID,BARCELONA,2019-04-18" + 
            " 05:50:00,2019-04-18 08:55:00,3.08,AVE,Preferente,68.95," + 
            "Promo,,{},2019-04-11 21:49:46";
 
    @Test
    public void testReadingOneLine() throws Exception {
        String[] lines = new CSVParser().parseLine(LINE);
    
        assertEquals("should be MADRID",
            "MADRID", lines[2]);

        assertEquals("should be BARCELONA",
            "BARCELONA", lines[3]);

    }

}





