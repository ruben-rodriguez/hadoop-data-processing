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
                        csvParser.schedules();
                        break;
                    case 2:
                        csvParser.countByOrigDest();
                        break;
                    case 3:
                        csvParser.countByVehicle();
                        break;
                    case 4:
                        csvParser.getMeanPrice();
                        break;
                    case 5:
                        csvParser.vehicleByLocation();
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
