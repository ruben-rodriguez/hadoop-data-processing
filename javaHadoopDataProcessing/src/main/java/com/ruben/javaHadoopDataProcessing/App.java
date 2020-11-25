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

    private static final String CSV_FILE_NAME = "sample.csv";

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

            System.out.println(cmd.getArgList().size());

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

                        vcCountJob.execute();

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }
                    
                    break;

                case "Locations":

                    LocationsCount.LocationsCountJob lcCountJob = new LocationsCount.LocationsCountJob(inputDir, outputDir);

                    try {

                        lcCountJob.execute();

                    } catch (Exception ex) {

                        System.out.println(ex.getMessage());

                    }
            
                default:
                    break;
            }
            

        } catch (org.apache.commons.cli.ParseException ex) {

            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp(App.class.getCanonicalName(), options );

        }
        
    }
}
