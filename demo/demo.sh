#!/bin/sh
set -e
set -x

DATA_SOURCE_PATH=/data-source
USER_WITH_MOVIE_AND_RATING_PATH=/user-movie_rating
MOVIES_OCCURRENCE_PATH=/movies-occurrence
NORMALIZED_MOVIE_OCCURRENCE_PATH=/normalized-movies-occurrence
DISTRIBUTED_MOVIE_RATINGS_PATH=/distributed-movie-ratings
EXPECTED_RATINGS_PATH=/expected-ratings

sh ~/start-hadoop.sh

# cleanup
hdfs dfs -rm -r ${DATA_SOURCE_PATH} || true
hdfs dfs -rm -r ${USER_WITH_MOVIE_AND_RATING_PATH} || true
hdfs dfs -rm -r ${MOVIES_OCCURRENCE_PATH} || true
hdfs dfs -rm -r ${NORMALIZED_MOVIE_OCCURRENCE_PATH} || true
hdfs dfs -rm -r ${DISTRIBUTED_MOVIE_RATINGS_PATH} || true
hdfs dfs -rm -r ${EXPECTED_RATINGS_PATH} || true

# import data
hdfs dfs -mkdir ${DATA_SOURCE_PATH}
hdfs dfs -put data/*.txt ${DATA_SOURCE_PATH}

# run
hadoop jar recommender-system-demo.jar \
  -DdataSourcePath=${DATA_SOURCE_PATH} \
  -DuserWithMovieRatingPath=${USER_WITH_MOVIE_AND_RATING_PATH} \
  -DmovieOccurrencePath=${MOVIES_OCCURRENCE_PATH} \
  -DnormalizedMovieOccurrencePath=${NORMALIZED_MOVIE_OCCURRENCE_PATH} \
  -DdistributedMovieRatingsPath=${DISTRIBUTED_MOVIE_RATINGS_PATH} \
  -DexpectedRatingsPath=${EXPECTED_RATINGS_PATH}
