#!/bin/bash

java -cp target/scala-2.11/movielens-assembly-1.jar movielens.StatsApp ./data/ml-1m/movies.dat ./data/ml-1m/ratings.dat 100

