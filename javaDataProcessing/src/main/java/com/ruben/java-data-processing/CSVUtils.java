package com.ruben.javaDataProcessing;

import java.io.FileReader; 
import java.util.List; 
import com.opencsv.*; 
import java.io.File;

public class CSVUtils { 
       
    public List<String[]> readAllData(String filename) 
    { 
        List<String[]> allData = null;

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());

        try { 
  
            // Create an object of filereader class 
            // with CSV file as a parameter. 
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
  
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
                                    
            allData = csvReader.readAll(); 
  
            // print Data 
            for (String[] row : allData) { 
                for (String cell : row) { 
                    System.out.print(cell + "\t"); 
                } 
                System.out.println(); 
            } 

            
        } 
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

        return allData;

    } 
}