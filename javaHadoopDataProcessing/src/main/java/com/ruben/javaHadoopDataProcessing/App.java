package com.ruben.javaHadoopDataProcessing;

import java.util.InputMismatchException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * 
 */
public class App 
{

    public static void main( String[] args )
    {
        int option;

        long startTime, endTime;

        Options options = new Options();  
        options.addOption("a", "app", true,  "Application to execute: Shedules, Vehicle, Locations");
        options.addOption("i", "input", true,  "Hadoop input dir ");
        options.addOption("o", "output", true,  "Hadoop output dir ");
        options.addOption("h", "help",  false, "Show help");

        try {

            BasicParser parser = new BasicParser();
            CommandLine cmd = parser.parse( options, args);

            if (cmd.hasOption("help")){
                new HelpFormatter().printHelp("javaHadoopDataProcessing", options, true );  
                return;  
            }  

            String app = cmd.getOptionValue("app");
            String inputDir = cmd.getOptionValue("input");
            String outputDir = cmd.getOptionValue("output");

            switch (app) {
                case "Vehicles":

                    VehicleCount.VehicleCountJob vcCountJob = new VehicleCount.VehicleCountJob(inputDir, outputDir);

                    try {

                        startTime = System.currentTimeMillis(); 

                        vcCountJob.execute();

                        endTime = System.currentTimeMillis(); 
                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime));

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }
                    
                    break;

                case "Locations":

                    LocationsCount.LocationsCountJob lcCountJob = new LocationsCount.LocationsCountJob(inputDir, outputDir);

                    try {

                        startTime = System.currentTimeMillis(); 

                        lcCountJob.execute();

                        endTime = System.currentTimeMillis(); 
                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime));

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }

                    break;
                
                case "Price":

                    MeanPrice.MeanPriceJob meanPriceJob = new MeanPrice.MeanPriceJob(inputDir, outputDir);

                    try {

                        startTime = System.currentTimeMillis(); 

                        meanPriceJob.execute();

                        endTime = System.currentTimeMillis(); 
                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime));

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }

                    break;

                case "Schedules":

                SchedulesCount.SchedulesCountJob schedulesJob = new SchedulesCount.SchedulesCountJob(inputDir, outputDir);

                    try {

                        startTime = System.currentTimeMillis(); 

                        schedulesJob.execute();

                        endTime = System.currentTimeMillis(); 
                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime));

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }

                    break;
            
                case "LocationsVehicles":

                LocationsVehiclesCount.LocationsVehiclesCountJob LocationsVehiclesJob = new LocationsVehiclesCount.LocationsVehiclesCountJob(inputDir, outputDir);

                    try {

                        startTime = System.currentTimeMillis(); 

                        LocationsVehiclesJob.execute();

                        endTime = System.currentTimeMillis(); 
                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime));

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }

                    break;


                default:
                    break;
            }
            

        } catch (org.apache.commons.cli.ParseException ex) {

            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp(App.class.getCanonicalName(), options );

        }
        
    }
}
