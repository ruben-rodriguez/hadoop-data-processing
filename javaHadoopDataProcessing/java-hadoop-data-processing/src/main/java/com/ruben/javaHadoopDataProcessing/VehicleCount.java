package com.ruben.javaHadoopDataProcessing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.*; 

public class VehicleCount {

  public static class VehicleCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, 
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
                        
            } catch (ParseException e) {
                e.printStackTrace(); 
            }
        }
        
    }
  }

  public static class VehicleCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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

  public static void VehicleCountJob(String[] args) throws Exception {

    Job job = new Job();
    job.setJarByClass(VehicleCount.class);

    job.setMapperClass(VehicleCountMapper.class);
    job.setReducerClass(VehicleCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    //Takes CSV input data and output target by args
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}