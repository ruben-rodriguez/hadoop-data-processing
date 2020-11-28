package com.ruben.javaHadoopDataProcessing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.opencsv.*; 

public class LocationsVehiclesCount {

  public static class LocationsVehiclesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = Logger.getLogger(LocationsVehiclesCountMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {

        //Skip CSV header
        if (key.get() > 0) {

            try {

                CSVParser parser = new CSVParser();
                String[] row = parser.parseLine(value.toString());
                String origin = "";
                String destination = "";
                String vehicle = "";
                

                if(!row[2].isEmpty() && !row[3].isEmpty() && !row[7].isEmpty()) {

                    origin = row[2];
                    destination = row[3];
                    vehicle = row[7];

                    context.write(new Text(origin+":"+vehicle), new IntWritable(1));
                    context.write(new Text(destination+":"+vehicle), new IntWritable(1));
        
                }
                        
            } catch (Exception e) { 
                e.printStackTrace(); 
            } 
        }
        
    }
  }

  public static class LocationsVehiclesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(LocationsVehiclesCountReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
        InterruptedException {

        int count = 0;
        
        for (IntWritable value : values) {

          count++;

        }
        
        context.write(key, new IntWritable(count));

    }
  }

  public static class LocationsVehiclesCountJob {

    private String inputDir;
    private String outputDir;

    public LocationsVehiclesCountJob (String input, String output) {

      this.inputDir = input;
      this.outputDir = output;

    }

    private Logger logger = Logger.getLogger(LocationsVehiclesCountJob.class);
    private IntWritable result = new IntWritable();

    public void execute() throws Exception {

      Job job = new Job();
      long startTime, endTime;

      job.setJarByClass(LocationsVehiclesCount.class);
  
      job.setMapperClass(LocationsVehiclesCountMapper.class);
      job.setReducerClass(LocationsVehiclesCountReducer.class);
  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
  
      //Takes CSV input data and output target by args
      FileInputFormat.addInputPath(job, new Path(this.inputDir));
      FileOutputFormat.setOutputPath(job, new Path(this.outputDir));
  
      startTime = System.currentTimeMillis();
      boolean success = job.waitForCompletion(true);
      //System.exit(job.waitForCompletion(true) ? 0 : 1);
      endTime = System.currentTimeMillis(); 

      logger.info("\n\tTime taken in milli seconds: "
      + (endTime - startTime));

      System.exit(success ? 0 : 1);
  
    }
  }
}