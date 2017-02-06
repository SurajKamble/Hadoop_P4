#!/bin/bash
spark-submit --master yarn-client AvgWordLength.py hdfs://hadoop2-0-0/data/1gram/googlebooks-eng-all-1gram-20120701-x
