#!/bin/bash

# STUDENT PARAMETERS
# change where your logging file is so it can be deleted
LOGGING_FILE=loggingFile.bin
CHECKPOINT_POSITION_FILE=loggingFileStatus.txt

## GLOBAL PARAMETERS
TREE_DIRECTORY=tmpdir
INPUT_FILE_NAME=test_inputs.txt
OUTPUT_FILE_NAME=output_test.txt
OUTPUT_FILE_NAME_RESUME=RESUMED_output_test.txt
OUTPUT_FILE_NAME_FINAL=FINAL_output_test.txt
INPUT_VERIFICATION_TEST=VERIFICATION_input.txt
OUTPUT_VERIFICATION_TEST=VERIFICATION_output.txt

#how long to wait before pkill
WAIT_KILL_TIME=5

mkdir -p $TREE_DIRECTORY
# delete everything inside
rm -f $TREE_DIRECTORY/*
# remove the logging file: STUDENTS CHANGE THIS 
rm $LOGGING_FILE $CHECKPOINT_POSITION_FILE

####
#### TEST FOR CRASH AND RECOVERY
####
echo "Now spawning the test program..."
# spawn the child process and save the PID for later
./test_logging_restore -d $TREE_DIRECTORY -m test -i $INPUT_FILE_NAME -o $OUTPUT_FILE_NAME -t $(wc -l < $INPUT_FILE_NAME) -c 1000 -p 100 & test_program_pid=$!
# sleep several seconds before attempting to kill
sleep $WAIT_KILL_TIME
echo "Now attempting to kill program..."
# then kill the program
kill -9 "$test_program_pid"
# wait a second for the program to clean up
sleep 1

TOTAL_LINES=$(wc -l < $INPUT_FILE_NAME)

# get where the program failed, note that this checks for newline characters, so any unfinished operations are not counted
num_lines_finished=$(wc -l < $OUTPUT_FILE_NAME)
echo "FOUND THAT $num_lines_finished OPERATIONS FINISHED BEFORE CRASH"

# now create a split file based on where we were
tail -n $(($TOTAL_LINES-$num_lines_finished)) "$INPUT_FILE_NAME" > $OUTPUT_FILE_NAME_RESUME

# restart the program and let it finish all of the operations after the crash
./test_logging_restore -d $TREE_DIRECTORY -m test -i $OUTPUT_FILE_NAME_RESUME -o $OUTPUT_FILE_NAME_FINAL -t $(wc -l < $OUTPUT_FILE_NAME_RESUME) -c 1000 -p 100

# now split both the resume file and the test file for the last 400 points to test the queries
tail -n 400 $OUTPUT_FILE_NAME_RESUME > $INPUT_VERIFICATION_TEST # truth file
tail -n 400 $OUTPUT_FILE_NAME_FINAL > $OUTPUT_VERIFICATION_TEST # the output from the program above

# now we can check for the difference
num_different_from_queries=$(diff -y --suppress-common-lines $INPUT_VERIFICATION_TEST $OUTPUT_VERIFICATION_TEST | grep '^' | wc -l)
num_total_exists=400
percentage_incorrect_queries=$(echo "100*$num_different_from_queries/400" | bc -l)

echo "INCORRECT QUERY RESULTS: $num_different_from_queries/400 ($percentage_incorrect_queries%) INCORRECT"
