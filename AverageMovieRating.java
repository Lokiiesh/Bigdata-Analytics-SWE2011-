import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageMovieRating {

    public static class AverageRatingMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length > 2) {
                try {
                    int movieId = Integer.parseInt(fields[1]);
                    float rating = Float.parseFloat(fields[2]);
                    context.write(new IntWritable(movieId), new FloatWritable(rating));
                } catch (NumberFormatException e) {
                
                    System.err.println("Error parsing record: " + value.toString());
                }
            }
        }
    }

    public static class AverageRatingReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }
            float average = sum / count;

    
            context.write(key, new FloatWritable(average));
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageMovieRating <input path> <output path>");
            System.exit(-1);
        }

    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Movie Rating");

   
        job.setJarByClass(AverageMovieRating.class);

    
        job.setMapperClass(AverageRatingMapper.class);
        job.setReducerClass(AverageRatingReducer.class);

   
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
