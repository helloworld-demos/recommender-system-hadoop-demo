package iam.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static iam.demos.Constants.DEFAULT_HADOOP_KEY_VALUE_DELIMITER;

public class Sum {
    public static void perform(Path distributedMovieRatings, Path expectedRatings) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setJarByClass(Sum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, distributedMovieRatings);
        TextOutputFormat.setOutputPath(job, expectedRatings);

        job.waitForCompletion(true);
    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: userA:movieA -> expectedRating
            // output key: userA:movieA
            // output val: expectedRating
            String[] vals = value.toString().split(DEFAULT_HADOOP_KEY_VALUE_DELIMITER);

            context.write(new Text(vals[0]), new DoubleWritable(Double.parseDouble(vals[1])));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            // input key: userA:movieA
            // input val: <movieA.rating, movieB.rating, movieC.rating, ...>
            // output key: userA:movieA
            // output val: expectedRating
            double expectedRating = 0;

            for (DoubleWritable value : values) {
                expectedRating += value.get();
            }

            context.write(key, new DoubleWritable(expectedRating));
        }
    }
}
