package edu.cshl.schatz.jnomics.io;

import edu.cshl.schatz.jnomics.util.ByteUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.msgpack.MessagePack;
import org.msgpack.type.ValueType;
import org.msgpack.unpacker.Unpacker;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * User: james
 */
public class MPRecordReader<R> extends RecordReader<MPHeader, R> {

    private BufferedByteReader bufferedReader;
    private FileSystem fs;
    
    private MessagePack msgPack = new MessagePack();

    private MPHeader mpHeader;

    private long fileSplitStart, fileSplitEnd;

    private byte[] delimiter;

    //use this buffer to seek to the next entry
    private LinkedList<Byte> seekBuffer = new LinkedList<Byte>();
    
    private LinkedList<Byte> chunkBuffer = new LinkedList<Byte>();

    private int queued_objects;
    private Unpacker objectUnpacker;
    
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        fs = FileSystem.get(taskAttemptContext.getConfiguration());
        FileSplit fileSplit = (FileSplit) inputSplit;
        FSDataInputStream inStream = fs.open(fileSplit.getPath());
        mpHeader = msgPack.read(inStream,MPHeader.class); // read header from beginning of file
        delimiter = mpHeader.delimiter.getBytes();
        bufferedReader = new BufferedByteReader(inStream);
        fileSplitStart = fileSplit.getStart();
        fileSplitEnd = fileSplit.getStart() + fileSplit.getLength();
        //just in case we happend to land exactly after a delimiter, backtrack to find the last one
        bufferedReader.seek(fileSplitStart - delimiter.length < 0 ? 0 : fileSplitStart - delimiter.length);
    }

    private void seekToNextEntry() throws IOException {
        seekBuffer.clear();
        for(int i=0;i<delimiter.length-1;i++)
            seekBuffer.add(bufferedReader.readByte());
        do{
            seekBuffer.add(bufferedReader.readByte());
            seekBuffer.pop();
        }while(!ByteUtil.reverseEndEqual(seekBuffer,delimiter));
    }

    private void readCompressedChunk() throws IOException{
        chunkBuffer.clear();
        do{
            chunkBuffer.add(bufferedReader.readByte());
        }while(!ByteUtil.reverseEndEqual(seekBuffer,delimiter));
        //remove delimiter from chunk
        for(int i=0;i<delimiter.length;i++){
            chunkBuffer.removeLast();
        }
        //get the count for the number of objects in the compressed block
        byte []c = new byte[4];
        for(int t=chunkBuffer.size()-1;t>=chunkBuffer.size()-4;t--){
            c[(t - (chunkBuffer.size() - 1))+3] = chunkBuffer.pop();
        }

        queued_objects = ByteBuffer.wrap(c).getInt();

        byte[] compressed = new byte[chunkBuffer.size()];
        Iterator<Byte> it = chunkBuffer.iterator();
        int j = 0;
        while(it.hasNext()){
            compressed[j] = it.next();
            j++;
        }
        byte[] uncompressed = Snappy.uncompress(compressed);

        objectUnpacker = msgPack.createUnpacker(new ByteArrayInputStream(uncompressed));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(queued_objects > 0)
            return true;
        if(bufferedReader.getPos() < fileSplitEnd){
            try{
                seekToNextEntry();
                readCompressedChunk();
            }catch(EOFException e){
                return false;
            }
        }
        if(queued_objects > 0)
            return true;
        return false;
    }

    @Override
    public MPHeader getCurrentKey() throws IOException, InterruptedException {
        return mpHeader;
    }

    @Override
    public R getCurrentValue() throws IOException, InterruptedException {
        if(queued_objects > 0){
            ValueType type = objectUnpacker.getNextType();
            queued_objects--;
            return (R)type;
        }
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (bufferedReader.getPos()-fileSplitStart) / (((float)fileSplitEnd) - fileSplitStart);
    }

    @Override
    public void close() throws IOException {
        bufferedReader.close();
    }
}
