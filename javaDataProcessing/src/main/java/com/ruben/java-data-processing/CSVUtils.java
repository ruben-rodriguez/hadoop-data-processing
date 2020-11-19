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
import java.text.DecimalFormat;


public class CSVUtils { 

    private String filename;
    private File file;

    public CSVUtils(String file) {

        this.filename = file;
        ClassLoader classLoader = getClass().getClassLoader();
        this.file = new File(classLoader.getResource(this.filename).getFile());

    }
      
    public void vehicleByLocation() {
                
        try { 
  
            Map<String, Map<String, Integer>> originVehicles = new HashMap<String, Map<String, Integer>>();
            Map<String, Map<String, Integer>> destinationVehicles = new HashMap<String, Map<String, Integer>>();
            String origin = "";
            String destination = "";
            String vehicle = "";

            FileReader filereader = new FileReader(this.file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
                                    
            String[] row;
            
            while ((row = csvReader.readNext()) != null) {

                if(!row[2].isEmpty() && !row[3].isEmpty() && !row[7].isEmpty()) {

                    origin = row[2];
                    destination = row[3];
                    vehicle = row[7];
                
                    if(!originVehicles.containsKey(origin)) {

                        Map<String, Integer> types = new HashMap<String, Integer>();
                        types.put(vehicle, 1);
                        originVehicles.put(origin, types);

                    } else {

                        Map<String, Integer> types = originVehicles.get(origin);

                        if(types.containsKey(vehicle)){

                            int count = types.get(vehicle);
                            types.put(vehicle, count + 1);

                        } else {

                            types.put(vehicle, 1);

                        }

                        originVehicles.put(origin, types);

                    }

                    if(!destinationVehicles.containsKey(destination)) {

                        Map<String, Integer> types = new HashMap<String, Integer>();
                        types.put(vehicle, 1);
                        destinationVehicles.put(destination, types);

                    } else {

                        Map<String, Integer> types = destinationVehicles.get(destination);

                        if(types.containsKey(vehicle)){

                            int count = types.get(vehicle);
                            types.put(vehicle, count + 1);

                        } else {

                            types.put(vehicle, 1);

                        }

                        destinationVehicles.put(destination, types);
                    }
                }
            }

            System.out.println();
            for (Map.Entry<String,  Map<String, Integer>> entry : originVehicles.entrySet()) {
                
                Map<String, Integer> values = entry.getValue();

                System.out.println("\tOrigin: " + entry.getKey());

                for (Map.Entry<String, Integer> value : values.entrySet()) {

                    System.out.println("\tType of Vehicle: " + value.getKey() + " count: " + value.getValue());

                }
                
            }

            System.out.println();
            for (Map.Entry<String,  Map<String, Integer>> entry : destinationVehicles.entrySet()) {
                
                Map<String, Integer> values = entry.getValue();

                System.out.println("\tDestination: " + entry.getKey());

                for (Map.Entry<String, Integer> value : values.entrySet()) {

                    System.out.println("\tType of Vehicle: " + value.getKey() + " count: " + value.getValue());

                }
            }

            //System.out.println(originVehicles.toString());
            //System.out.println(destinationVehicles.toString());
            
        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

    }


    public void getMeanPrice() {
        
        try { 
  
            Map<String, List<Double>> map = new HashMap<String, List<Double>>();
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
                                    
            String[] row;
            
            while ((row = csvReader.readNext()) != null) {

                if(!row[8].isEmpty() && !row[9].isEmpty()) {

                    String fare = row[8];
                    Double rowPrice = Double.parseDouble(row[9]);
                    
                    if(map.containsKey(fare)) {
                        map.get(fare).add(rowPrice);
                    } else {
                        List<Double> values = new ArrayList<Double>();
                        values.add(rowPrice);
                        map.put(fare, values);
                    }

                }
               
            }

            DecimalFormat df = new DecimalFormat("#.##");      

            for (Map.Entry<String, List<Double>> entry : map.entrySet()) {
                
                List<Double> values = entry.getValue();
                Double total = 0.0;
                
                for (int i = 0; i < values.size(); i++) {
                    total += values.get(i);
                }
                Double mean = total / values.size();
                Double formatted = Double.valueOf(df.format(mean));
                System.out.println("\tClass: " + entry.getKey() + " mean price: " + formatted);
              }
            
            //System.out.println(map.toString());
            
        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

    }


    public List<String[]> readAllData() {

        List<String[]> allData = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(this.filename).getFile());

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

    public void countByVehicle() {

        try { 
  
            Map<String, Integer> vehicles = new HashMap<String,Integer>();
            FileReader filereader = new FileReader(file.getAbsolutePath()); 
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 

            String[] row;

            while ((row = csvReader.readNext()) != null) { 

                if(!row[7].isEmpty()) {

                    String vehicle = row[7];
                
                    if(vehicles.containsKey(vehicle)) {
                        int count = vehicles.get(vehicle);
                        vehicles.put(vehicle, count + 1);
                    } else {
                        vehicles.put(vehicle, 1);
                    }
    
                }

            }

            System.out.println();
            int mostFreqVehicle=(Collections.max(vehicles.values())); 
            for (Entry<String, Integer> entry : vehicles.entrySet()) { 
                if (entry.getValue() == mostFreqVehicle) {
                    System.out.println("\tMost frecuent type of vehicle: " + entry.getKey() + " with " + mostFreqVehicle + " counts.");     // Print the key with max value
                }
            }

            printMap(vehicles);

        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

    }

    public void countByOrigDest() {

        try { 
  
            Map<String, Integer> origins = new HashMap<String,Integer>();
            Map<String, Integer> destinations = new HashMap<String,Integer>();
            FileReader filereader = new FileReader(this.file.getAbsolutePath()); 
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
            System.out.println("\tValue: " + str + " count: " + map.get(str));
        }

    }
}