package com.ruben.javaHadoopDataProcessing;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class LocationsCount {

  public static class LocationsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = Logger.getLogger(LocationsCountMapper.class);

    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {

        //Skip CSV header
        if (key.get() > 0) {

            try {

                CSVParser parser = new CSVParser();
                String[] row = parser.parseLine(value.toString());
                String origin = "";
                String destination = "";

                if(!row[2].isEmpty() && !row[3].isEmpty()) {

                    origin = row[2];
                    destination = row[3];

                    context.write(new Text("O:"+ origin), new IntWritable(1));
                    context.write(new Text("D:" + destination), new IntWritable(1));
        
                }
                        
            } catch (Exception e) { 
                e.printStackTrace(); 
            } 
        }
        
    }
  }

  public static class LocationsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(LocationsCountReducer.class);
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
        InterruptedException {

        int count = 0;
        
        for (IntWritable value : values) {

          count++;

        }
        
        context.write(key, new IntWritable(count));

    }
  }

  public static class LocationsCountJob {

    private String inputDir;
    private String outputDir;

    public LocationsCountJob (String input, String output) {

      this.inputDir = input;
      this.outputDir = output;

    }

    private Logger logger = Logger.getLogger(LocationsCountJob.class);
    private IntWritable result = new IntWritable();

    public void execute() throws Exception {

      Job job = new Job();
      job.setJarByClass(LocationsCount.class);
  
      job.setMapperClass(LocationsCountMapper.class);
      job.setReducerClass(LocationsCountReducer.class);
  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
  
      //Takes CSV input data and output target by args
      FileInputFormat.addInputPath(job, new Path(this.inputDir));
      FileOutputFormat.setOutputPath(job, new Path(this.outputDir));
  
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  
    }
  }
}