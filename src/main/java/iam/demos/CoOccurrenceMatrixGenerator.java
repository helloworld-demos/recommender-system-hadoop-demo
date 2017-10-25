package iam.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static iam.demos.Constants.*;

public class CoOccurrenceMatrixGenerator {
    public static void perform(Path userWithMovieRating, Path moviesOccurrence) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, userWithMovieRating);
        TextOutputFormat.setOutputPath(job, moviesOccurrence);

        job.waitForCompletion(true);
    }

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // key is ignored normally, since it is most time a line no
            // input val: userId -> movieId_A:rating_A,movieId_B:rating_B
            // output key: movieId_A:movieId_B
            // output val: 1
            String[] movieRatings = value.toString().split(DEFAULT_HADOOP_KEY_VALUE_DELIMITER)[1].split(DEFAULT_VALUES_DELIMITER);

            for (String movieRating : movieRatings) {
                String movieId = movieRating.split(DEFAULT_APPLICATION_KEY_VALUE_DELIMITER)[0];

                for (String anotherMovieRating : movieRatings) {
                    String anotherMovieId = anotherMovieRating.split(DEFAULT_APPLICATION_KEY_VALUE_DELIMITER)[0];

                    context.write(new Text(movieId + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + anotherMovieId), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // input key: movieA:movieB
            // input val: <1,1,1...>
            // output key: movieA:movieB
            // output val: 3
            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));
        }
    }
}
