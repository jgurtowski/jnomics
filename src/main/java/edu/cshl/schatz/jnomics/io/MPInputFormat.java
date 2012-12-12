package edu.cshl.schatz.jnomics.io;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author james
 * A FileInputFormat for reading MessagePacked Data
 * @return MPInputFormatRecordReader
 */
public class MPInputFormat<R> extends FileInputFormat<MPHeader,R> {

	@Override
	public RecordReader<MPHeader,R> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {

        return new MPRecordReader<R>();
	}

}
