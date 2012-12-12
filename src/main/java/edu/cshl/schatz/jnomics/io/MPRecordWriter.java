package edu.cshl.schatz.jnomics.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.msgpack.MessagePack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * User: james
 */
public class MPRecordWriter<K, V> extends RecordWriter<K,V> {


    private boolean first = true;
    private MessagePack msgPack = new MessagePack();
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private int BUFFER_SIZE = 64 * 1024;
    
    private FSDataOutputStream outStream;


    public MPRecordWriter(FSDataOutputStream out){
        outStream = out;
        msgPack.createPacker(buffer);
    }

    public void flush(){

    }

    @Override
    public void write(K k, V v) throws IOException, InterruptedException {
        if(first){

        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outStream.close();
    }
}
