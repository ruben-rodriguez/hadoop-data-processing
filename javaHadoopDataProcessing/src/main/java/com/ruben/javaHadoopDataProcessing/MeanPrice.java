package com.ruben.javaHadoopDataProcessing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.opencsv.*; 

public class MeanPrice {

  public static class MeanPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(MeanPriceMapper.class);

    private final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {

        //Skip CSV header
        if (key.get() > 0) {

            try {

                CSVParser parser = new CSVParser();
                String[] row = parser.parseLine(value.toString());

                if(!row[8].isEmpty() && !row[9].isEmpty()) {

                    String fare = row[8];
                    Double rowPrice = Double.parseDouble(row[9]);

                    context.write(new Text(fare), new DoubleWritable(rowPrice));

                }
                        
            } catch (Exception e) { 
                e.printStackTrace(); 
            } 
        }
        
    }
  }

  public static class MeanPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(MeanPriceReducer.class);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, 
        InterruptedException {

        Double sum = 0.0;
        int count = 0;
        
        for (DoubleWritable value : values) {

          sum = value.get() + sum;
          count++;

        }

        Double total = sum / count;
        
        context.write(key, new DoubleWritable(total));

    }
  }

  public static class MeanPriceJob {

    private String inputDir;
    private String outputDir;

    public MeanPriceJob (String input, String output) {

      this.inputDir = input;
      this.outputDir = output;

    }

    private Logger logger = Logger.getLogger(MeanPriceJob.class);

    public void execute() throws Exception {

      Job job = new Job();
      job.setJarByClass(MeanPrice.class);
  
      job.setMapperClass(MeanPriceMapper.class);
      job.setReducerClass(MeanPriceReducer.class);
  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
  
      //Takes CSV input data and output target by args
      FileInputFormat.addInputPath(job, new Path(this.inputDir));
      FileOutputFormat.setOutputPath(job, new Path(this.outputDir));
  
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  
    }
  }
}