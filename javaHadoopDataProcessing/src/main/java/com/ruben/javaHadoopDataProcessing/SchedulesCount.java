package com.ruben.javaHadoopDataProcessing;

import java.io.IOException;
import java.text.SimpleDateFormat;  
import java.util.Date;  

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

public class SchedulesCount {

  public static class SchedulesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = Logger.getLogger(SchedulesCountMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {

        //Skip CSV header
        if (key.get() > 0) {

            try {

                CSVParser parser = new CSVParser();
                String[] row = parser.parseLine(value.toString());
                

                if(!row[5].isEmpty()) {

                    String date = row[5];
                    Date departure = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(date);

                    SimpleDateFormat weekDayFormat = new SimpleDateFormat("EEEE");
                    SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");
                    SimpleDateFormat combinedFormat = new SimpleDateFormat("EEEE HH:mm");

                    String weekDay = weekDayFormat.format(departure).toString();
                    String time = timeFormat.format(departure).toString();
                    String combinedDate = combinedFormat.format(departure).toString();

                    context.write(new Text(weekDay), new IntWritable(1));
                    context.write(new Text(time), new IntWritable(1));
                    context.write(new Text(combinedDate), new IntWritable(1));
        
                }
                        
            } catch (Exception e) { 
                e.printStackTrace(); 
            } 
        }
        
    }
  }

  public static class SchedulesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(SchedulesCountReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
        InterruptedException {

        int count = 0;
        
        for (IntWritable value : values) {

          count++;

        }
        
        context.write(key, new IntWritable(count));

    }
  }

  public static class SchedulesCountJob {

    private String inputDir;
    private String outputDir;

    public SchedulesCountJob (String input, String output) {

      this.inputDir = input;
      this.outputDir = output;

    }

    private Logger logger = Logger.getLogger(SchedulesCountJob.class);
    private IntWritable result = new IntWritable();

    public void execute() throws Exception {

      Job job = new Job();
      long startTime, endTime;

      job.setJarByClass(SchedulesCount.class);
  
      job.setMapperClass(SchedulesCountMapper.class);
      job.setReducerClass(SchedulesCountReducer.class);
  
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