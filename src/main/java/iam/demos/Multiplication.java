package iam.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static iam.demos.Constants.*;

public class Multiplication {
    private static final Logger logger = Logger.getLogger(Multiplication.class);

    public static void perform(Path normalizedMovieOccurrencePath, Path dataSourcePath, Path distributedMovieRatings) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, normalizedMovieOccurrencePath, TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, dataSourcePath, TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, distributedMovieRatings);

        job.waitForCompletion(true);
    }

    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input values: movieB -> movieA:5, movieC -> movieA:10
            // output key: movieB
            // output val: movie:5
            String[] vals = value.toString().split(DEFAULT_HADOOP_KEY_VALUE_DELIMITER);

            context.write(new Text(vals[0]),
                          new Text(vals[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: userId, movieId, rating
            // output key: movieId
            // output val: R:userId:rating
            String[] userMovieRating = value.toString().trim().split(DEFAULT_VALUES_DELIMITER);

            if (userMovieRating.length != 3) {
                logger.warn(String.format("%s has wrong format, it should be like userId,movieId,rating", value.toString()));
                return;
            }

            String userId = userMovieRating[0].trim();
            String movieId = userMovieRating[1].trim();
            String rating = userMovieRating[2].trim();

            context.write(new Text(movieId),
                          new Text(MOVIE_RATING_IDENTIFIER + userId + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + rating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input key: movieB
            // input vals: movieA:normalizedCountA, movieB:normalizedCountB,...,R:userIdA:ratingA,R:userIdB:ratingB
            // output key: userA:movieA
            // output vals: normalizedCountA * ratingB

            // TODO how large could those data be?

            Map<String, Double> movieToNormalizedCounts = new HashMap<>();
            Map<String, Double> userToRatings = new HashMap<>();

            for (Text value : values) {
                String item = value.toString();
                String[] vals = item.split(DEFAULT_APPLICATION_KEY_VALUE_DELIMITER);

                if (item.startsWith(MOVIE_RATING_IDENTIFIER)) {
                    userToRatings.put(vals[1], Double.parseDouble(vals[2]));
                } else {
                    movieToNormalizedCounts.put(vals[0], Double.parseDouble(vals[1]));
                }
            }

            for (Map.Entry<String, Double> movie2NormalizedCount : movieToNormalizedCounts.entrySet()) {
                String movieId = movie2NormalizedCount.getKey();
                double normalizedCount = movie2NormalizedCount.getValue();

                for (Map.Entry<String, Double> user2Rating : userToRatings.entrySet()) {
                    String userId = user2Rating.getKey();
                    double rating = user2Rating.getValue();

                    context.write(new Text(userId + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + movieId),
                                  new DoubleWritable(normalizedCount * rating));
                }
            }
        }
    }
}
