package com.ruben.javaDataProcessing;

import java.util.InputMismatchException;
import java.util.Scanner;

/**
 * 
 */
public class App 
{

    private static final String CSV_FILE_NAME = "sample.csv";

    public static void main( String[] args )
    {
        Scanner sn = new Scanner(System.in);
        boolean exit = false;
        int option;

        long startTime, endTime;

        while (!exit) {

            System.out.println();
            System.out.println("\t1 - Most frequent schedules.");
            System.out.println("\t2 - Most frequent trip origins and destinations.");
            System.out.println("\t3 - Most frequent type of vehicle.");
            System.out.println("\t4 - Mean price per trip class.");
            System.out.println("\t5 - Type of train by origin and destination.");
            System.out.println("\t6 - Exit\n");
        
            try {

                CSVUtils csvParser = new CSVUtils(CSV_FILE_NAME);
                System.out.print("\tEnter a valid option from menu: ");
                option = sn.nextInt();

                switch (option) {
                    case 1:
                        startTime = System.currentTimeMillis(); 

                        csvParser.schedules();

                        endTime = System.currentTimeMillis(); 

                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime)); 

                        break;
                    case 2:
                        startTime = System.currentTimeMillis();

                        csvParser.countByOrigDest();

                        endTime = System.currentTimeMillis(); 

                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime)); 

                        break;
                    case 3:
                        startTime = System.currentTimeMillis();

                        csvParser.countByVehicle();

                        endTime = System.currentTimeMillis(); 

                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime)); 

                        break;
                    case 4:
                        startTime = System.currentTimeMillis();

                        csvParser.getMeanPrice();

                        endTime = System.currentTimeMillis(); 

                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime)); 

                        break;
                    case 5:
                        startTime = System.currentTimeMillis();

                        csvParser.vehicleByLocation();

                        endTime = System.currentTimeMillis(); 

                        System.out.println("\n\tTime taken in milli seconds: "
                           + (endTime - startTime)); 

                        break;
                    case 6:
                        exit = true;
                        break;
                    default:
                        System.out.println("\t\nOnly option in menu range accepted!\n");
                }
            } catch (InputMismatchException e) {
                System.out.println("\t\nNot a valid option!\n");
                sn.next();
            }
        }
    }
}
