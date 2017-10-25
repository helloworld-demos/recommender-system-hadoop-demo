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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

import static iam.demos.Constants.DEFAULT_APPLICATION_KEY_VALUE_DELIMITER;
import static iam.demos.Constants.DEFAULT_VALUES_DELIMITER;

public class DataDividerByUser {
    private static final Logger logger = Logger.getLogger(DataDividerByUser.class);

    public static void perform(Path userMovieRating, Path userWithMovieRating) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, userMovieRating);
        TextOutputFormat.setOutputPath(job, userWithMovieRating);

        job.waitForCompletion(true);
    }

    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: userId, movieId, rating
            // output key: userId
            // output val: rating:movieId:rating
            String[] userMovieRating = value.toString().trim().split(DEFAULT_VALUES_DELIMITER);

            if (userMovieRating.length != 3) {
                logger.warn(String.format("%s has wrong format, it should be like userId,movieId,rating", value.toString()));
                return;
            }

            String userId = userMovieRating[0].trim();
            String movieId = userMovieRating[1].trim();
            String rating = userMovieRating[2].trim();

            context.write(new IntWritable(Integer.parseInt(userId)), new Text(movieId + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // key -> userId
            // values -> movieId_A:rating_A, movieId_B:rating_B
            // output key -> userID
            // output val -> movieId_A:rating_A,movieId_B:rating_B
            Iterator<Text> iter = values.iterator();

            StringBuilder sb = new StringBuilder();

            while (iter.hasNext()) {
                sb.append(iter.next().toString())
                  .append(DEFAULT_VALUES_DELIMITER);
            }

            // remove last character which is MOVIE_RATING_COLUMN_DELIMITER
            sb.setLength(sb.length() - 1);

            context.write(key, new Text(sb.toString()));
        }
    }
}
