package com.ruben.javaDataProcessing;

import java.io.FileReader; 
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.ArrayList; 
import java.util.Collections;
import java.util.Arrays;
import com.opencsv.*; 
import java.io.File;

public class CSVUtils { 
      
    public double getMinMaxPrice(String filename) {
        
        List<String[]> allData =  new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());
        double maxPrice = 0;
        double minPrice = 0;
        
        try { 
  
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
                                    
            String[] row;
            
            while ((row = csvReader.readNext()) != null) {

                if(!row[9].isEmpty()) {

                    double rowPrice = Double.parseDouble(row[9]);

                    if(rowPrice > maxPrice)
                        maxPrice = rowPrice;

                    if(rowPrice < minPrice)
                        minPrice = rowPrice;

                }
               
            }
            
        } 
        catch (Exception e) { 
            e.printStackTrace(); 
        } 
        
        System.out.println("\tMax trip price: " +maxPrice + "\n");
        return maxPrice;
    }


    public List<String[]> readAllData(String filename) {

        List<String[]> allData = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());

        try { 
  
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
                                    
            allData = csvReader.readAll(); 
            
        } 
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

        return allData;

    } 

    public void countByOrigDest(String filename){

        List<String[]> allData = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());

        try { 
  
            Map<String, Integer> origins = new HashMap<String,Integer>();
            Map<String, Integer> destinations = new HashMap<String,Integer>();
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 

            String[] row;

            while ((row = csvReader.readNext()) != null) {   

              if(!row[2].isEmpty()) {

                String location = row[2];
            
                if(origins.containsKey(location)) {
                    int count = origins.get(location);
                    origins.put(location, count + 1);
                } else {
                    origins.put(location, 1);
                }

              }

              if(!row[3].isEmpty()) {

                String location = row[3];
            
                if(destinations.containsKey(location)) {
                    int count = destinations.get(location);
                    destinations.put(location, count + 1);
                } else {
                    destinations.put(location, 1);
                }

              }
          
            }

            System.out.println();
            int maxValueInOrigMap=(Collections.max(origins.values())); 
            for (Entry<String, Integer> entry : origins.entrySet()) { 
                if (entry.getValue() == maxValueInOrigMap) {
                    System.out.println("\tMost frecuent origin: " + entry.getKey() + " with " + maxValueInOrigMap + " counts.");     // Print the key with max value
                }
            }

            int maxValueInDstMap=(Collections.max(destinations.values())); 
            for (Entry<String, Integer> entry : destinations.entrySet()) {  
                if (entry.getValue() == maxValueInDstMap) {
                    System.out.println("\tMost frecuent destination: " + entry.getKey() + " with " + maxValueInDstMap + " counts.");     // Print the key with max value
                }
            }

            printMap(origins);
            printMap(destinations);

        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 
        
    }

    public void printMap(Map<String, Integer> map){

        Set < String > keys = map.keySet(); 
        TreeSet < String > sortedKeys = new TreeSet < > (keys);
        for (String str: sortedKeys) {
            System.out.println("\tOrigin: " + str + " count: " + map.get(str));
        }

    }
}