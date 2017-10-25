package iam.demos;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Driver {
    private static final String EXPECTED_RATINGS_PATH_KEY = "expectedRatingsPath";
    private static final Logger logger = Logger.getLogger(Driver.class);
    private static final String DATA_SOURCE_PATH_KEY = "dataSourcePath";
    private static final String USER_WITH_MOVIE_AND_RATING_PATH_KEY = "userWithMovieRatingPath";
    private static final String MOVIES_OCCURRENCE_PATH_KEY = "movieOccurrencePath";
    private static final String NORMALIZED_MOVIES_OCCURRENCE_PATH_KEY = "normalizedMovieOccurrencePath";
    private static final String DISTRIBUTED_MOVIE_RATINGS_PATH_KEY = "distributedMovieRatingsPath";

    public static void main(String[] args) throws Exception {
        Map<String, String> arguments = new HashMap<>();

        for (String arg : args) {
            if (arg.startsWith("-D")) {
                String[] pair = arg.replace("-D", "").split("=");
                arguments.put(pair[0], pair[1]);
                logger.info(String.format("Receive parameter pair %s:%s", pair[0], pair[1]));
            }
        }

        //if (!arguments.containsKey(INPUT_PATH_KEY) || !arguments.containsKey(TEMP_PATH_KEY)) {
        //    throw new Error(String.format("%s and %s must exist.", INPUT_PATH_KEY, TEMP_PATH_KEY));
        //}

        DataDividerByUser.perform(new Path(arguments.get(DATA_SOURCE_PATH_KEY)), new Path(arguments.get(USER_WITH_MOVIE_AND_RATING_PATH_KEY)));

        CoOccurrenceMatrixGenerator.perform(new Path(arguments.get(USER_WITH_MOVIE_AND_RATING_PATH_KEY)), new Path(arguments.get(MOVIES_OCCURRENCE_PATH_KEY)));

        Normalize.perform(new Path(arguments.get(MOVIES_OCCURRENCE_PATH_KEY)), new Path(arguments.get(NORMALIZED_MOVIES_OCCURRENCE_PATH_KEY)));

        Multiplication.perform(new Path(arguments.get(NORMALIZED_MOVIES_OCCURRENCE_PATH_KEY)),
                               new Path(arguments.get(DATA_SOURCE_PATH_KEY)),
                               new Path(arguments.get(DISTRIBUTED_MOVIE_RATINGS_PATH_KEY)));


        Sum.perform(new Path(arguments.get(DISTRIBUTED_MOVIE_RATINGS_PATH_KEY)),
                    new Path(arguments.get(EXPECTED_RATINGS_PATH_KEY)));

        // TODO import result to db
    }
}
