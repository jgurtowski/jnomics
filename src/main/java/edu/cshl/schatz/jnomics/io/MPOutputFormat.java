package edu.cshl.schatz.jnomics.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class MPOutputFormat<K,V> extends FileOutputFormat<K,V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        return new MPRecordWriter<K,V>(fs.create(getDefaultWorkFile(taskAttemptContext, "")));
    }

}
