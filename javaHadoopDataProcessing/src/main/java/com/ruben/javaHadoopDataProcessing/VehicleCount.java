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

public class VehicleCount {

  public class VehicleCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Logger logger = Logger.getLogger(VehicleCountMapper.class);

    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {

        //Skip CSV header
        if (key.get() > 0) {

            try {

              CSVParser parser = new CSVParser();
              String[] row = parser.parseLine(value.toString());

                if(!row[7].isEmpty()) {

                    String vehicle = row[7];
                    context.write(new Text(vehicle), new IntWritable(1));
        
                }
                        
            } catch (Exception e) { 
                e.printStackTrace(); 
            } 
        }
        
    }
  }

  public class VehicleCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(VehicleCountReducer.class);
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


  public class VehicleCountJob {

    private String inputDir;
    private String outputDir;

    public VehicleCountJob (String input, String output) {

      this.inputDir = input;
      this.outputDir = output;

    }

    private Logger logger = Logger.getLogger(VehicleCountJob.class);
    private IntWritable result = new IntWritable();

    public void execute() throws Exception {

      Job job = new Job();
      job.setJarByClass(VehicleCount.class);
  
      job.setMapperClass(VehicleCountMapper.class);
      job.setReducerClass(VehicleCountReducer.class);
  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
  
      //Takes CSV input data and output target by args
      FileInputFormat.addInputPath(job, new Path(this.inputDir));
      FileOutputFormat.setOutputPath(job, new Path(this.outputDir));
  
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  
    }
  }
}