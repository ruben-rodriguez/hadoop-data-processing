package com.ruben.javaDataProcessing;

import java.io.BufferedReader; 
import java.io.Reader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.text.SimpleDateFormat;  
import java.util.Date;  


public class CSVUtils { 

    private String filename;
    private Reader inputFile;

    public CSVUtils(String filename) {

        this.filename = filename;
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(this.filename).getFile());
        this.inputFile = new InputStreamReader(classLoader.getResourceAsStream(this.filename)); 

    }


    public void schedules() {

        try {
            
            Map<String, Integer> weekdays = new HashMap<String,Integer>();
            Map<String, Integer> times = new HashMap<String,Integer>();
            BufferedReader filereader = new BufferedReader(this.inputFile);
            CSVReader csvReader = new CSVReaderBuilder(filereader) 
                                      .withSkipLines(1) 
                                      .build(); 
            
            String[] row;
        
            while ((row = csvReader.readNext()) != null) {

                if(!row[5].isEmpty()) {

                    String date = row[5];
                    Date departure = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(date);

                    SimpleDateFormat weekDayFormat = new SimpleDateFormat("EEEE");
                    SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");

                    String weekDay = weekDayFormat.format(departure).toString();
                    String time = timeFormat.format(departure).toString();
                    
                    if(weekdays.containsKey(weekDay)) {
                        int count = weekdays.get(weekDay);
                        weekdays.put(weekDay, count + 1);
                    } else {
                        weekdays.put(weekDay, 1);
                    }

                    if(times.containsKey(time)) {
                        int count = times.get(time);
                        times.put(time, count + 1);
                    } else {
                        times.put(time, 1);
                    }
    
                }

            }

            System.out.println();
            int mostFreqWeekday=(Collections.max(weekdays.values())); 
            for (Entry<String, Integer> entry : weekdays.entrySet()) { 
                if (entry.getValue() == mostFreqWeekday) {
                    System.out.println("\tMost frecuent day of the week: " + entry.getKey() + " with " + mostFreqWeekday + " counts.");     // Print the key with max value
                }
            }
            printMap(weekdays);

            System.out.println();
            int mostFreqTime=(Collections.max(times.values())); 
            for (Entry<String, Integer> entry : times.entrySet()) { 
                if (entry.getValue() == mostFreqTime) {
                    System.out.println("\tMost frecuent time of trip is: " + entry.getKey() + " with " + mostFreqTime + " counts.");     // Print the key with max value
                }
            }
            printMap(times);

        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 
    }
      
    public void vehicleByLocation() {
                
        try { 
  
            Map<String, Map<String, Integer>> originVehicles = new HashMap<String, Map<String, Integer>>();
            Map<String, Map<String, Integer>> destinationVehicles = new HashMap<String, Map<String, Integer>>();
            String origin = "";
            String destination = "";
            String vehicle = "";

            BufferedReader filereader = new BufferedReader(this.inputFile);
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
            
        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

    }


    public void getMeanPrice() {
        
        try { 
  
            Map<String, List<Double>> map = new HashMap<String, List<Double>>();

            BufferedReader filereader = new BufferedReader(this.inputFile);
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
                        
        }
        catch (Exception e) { 
            e.printStackTrace(); 
        } 

    }


    public List<String[]> readAllData() {

        List<String[]> allData = new ArrayList<>();

        try { 
  
            BufferedReader filereader = new BufferedReader(this.inputFile);
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

            BufferedReader filereader = new BufferedReader(this.inputFile);
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

            BufferedReader filereader = new BufferedReader(this.inputFile);
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