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
        System.out.println("Hello World!");
        Scanner sn = new Scanner(System.in);
        boolean exit = false;
        int option;

        while (!exit) {

            System.out.println("1. Func 1");
            System.out.println("2. Func 2");
            System.out.println("3. Func 3");
            System.out.println("4. Exit");
        
            try {

                CSVUtils csvParser = new CSVUtils();
                System.out.println("Enter a valid option from menu:");
                option = sn.nextInt();

                switch (option) {
                    case 1:
                        System.out.println("Func 1!");
                        List<String[]> test = csvParser.readAllData(CSV_FILE_NAME);
                        break;
                    case 2:
                        System.out.println("Func 2!");
                        break;
                    case 3:
                        System.out.println("Func 3!");
                        break;
                    case 4:
                        exit = true;
                        break;
                    default:
                        System.out.println("Only number in menu range");
                }
            } catch (InputMismatchException e) {
                System.out.println("Number expected!");
                sn.next();
            }
        }
    }
}
