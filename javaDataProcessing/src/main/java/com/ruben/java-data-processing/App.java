package com.ruben.javaDataProcessing;

import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.List; 

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
            System.out.println("\t1 - Get Min and Max Trip prices!");
            System.out.println("\t2 - Origins and Destination counts!");
            System.out.println("\t3 - Print all Data");
            System.out.println("\t4 - Exit\n");
        
            try {

                CSVUtils csvParser = new CSVUtils();
                System.out.print("\tEnter a valid option from menu: ");
                option = sn.nextInt();

                switch (option) {
                    case 1:
                        System.out.println("\n\tCalculating Min and Max Trip prices...\n\n");
                        Double maxPrice = csvParser.getMinMaxPrice(CSV_FILE_NAME);
                        break;
                    case 2:
                        csvParser.countByOrigDest(CSV_FILE_NAME);
                        break;
                    case 3:
                        System.out.println("\n\tPrinting all data...\n\n");
                        List<String[]> test = csvParser.readAllData(CSV_FILE_NAME);
                        for (String[] row : test) { 
                           for (String cell : row) { 
                                System.out.print(cell + "\t"); 
                            } 
                            System.out.println(); 
                        } 
                        break;
                    case 4:
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
