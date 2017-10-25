package iam.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static iam.demos.Constants.DEFAULT_APPLICATION_KEY_VALUE_DELIMITER;
import static iam.demos.Constants.DEFAULT_HADOOP_KEY_VALUE_DELIMITER;

public class Normalize {

    public static void perform(Path moviesOccurrence, Path nomarlizedMoviesOccurrence) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, moviesOccurrence);
        TextOutputFormat.setOutputPath(job, nomarlizedMoviesOccurrence);

        job.waitForCompletion(true);
    }

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input val: movieA:movieB -> 3
            // output key: movieA
            // output val: movieB:3

            String[] relationToCount = value.toString().split(DEFAULT_HADOOP_KEY_VALUE_DELIMITER);
            String[] movies = relationToCount[0].split(DEFAULT_APPLICATION_KEY_VALUE_DELIMITER);

            context.write(new Text(movies[0]), new Text(movies[1] + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + relationToCount[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input key: movieA
            // input val: <movieB:5, movieC:10>
            // TODO what if the movie is not watched (how does it work)
            // output key value: movieB -> movieA:5, movieC -> movieA:10

            Map<String, Double> history = new HashMap<>();
            double sum = 0;

            for (Text item : values) {
                String[] vals = item.toString().split(DEFAULT_APPLICATION_KEY_VALUE_DELIMITER);
                String otherMovie = vals[0];
                Double count = Double.parseDouble(vals[1]);

                history.put(otherMovie, count);

                sum += count;
            }

            for (Map.Entry<String, Double> pair : history.entrySet()) {
                context.write(
                        new Text(pair.getKey()),
                        new Text(key.toString() + DEFAULT_APPLICATION_KEY_VALUE_DELIMITER + pair.getValue() / sum)
                );
            }
        }
    }
}
