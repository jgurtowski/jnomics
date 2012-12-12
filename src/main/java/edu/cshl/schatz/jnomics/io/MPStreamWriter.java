package edu.cshl.schatz.jnomics.io;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.UUID;

/**
 * User: james
 */

public class MPStreamWriter{

    public static int DEFAULT_BUFFER_SIZE = 64 * 1024;
    
    private MPHeader header = new MPHeader();
    private OutputStream outStream;

    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    
    private Packer packer;

    private boolean initialized = false;

    private int header_length;

    private int buffer_size;

    private int num_records; // the number of records in this batch
    
    public MPStreamWriter(OutputStream outputStream){
        this(outputStream, new HashMap<String, String>());
    }
    
    public MPStreamWriter(OutputStream outputStream,
                          HashMap<String,String> headerUserData){
        this(outputStream, headerUserData,DEFAULT_BUFFER_SIZE);
    }
    
    
    public MPStreamWriter(OutputStream outputStream,
                          HashMap<String,String> headerUserData,
                          int bufferSize){
        outStream = outputStream;
        header.compression_codec = "snappy";
        buffer_size = bufferSize;
        header.delimiter = UUID.randomUUID().toString();
        header.userdata = headerUserData;
        packer = new MessagePack().createPacker(buffer);
        header_length = header.delimiter.getBytes().length;
    }


    public void initialize() throws IOException {
        packer.write(header);
        outStream.write(buffer.toByteArray());
        buffer.reset();
        initialized = true;
    }

    public void flush() throws IOException {
        if(buffer.size() > 0){
            outStream.write(header.delimiter.getBytes());
            outStream.write(Snappy.compress(buffer.toByteArray()));
            outStream.write(num_records); // write the number of records after the compressed stream
            buffer.reset();
        }
    }

    public void addRecord(Object record) throws IOException {
        if(!initialized)
            initialize();
        if(buffer.size() > buffer_size)
            flush();
        packer.write(record);
        num_records++;
    }

    public void close() throws IOException {
        flush();
        outStream.write(header.delimiter.getBytes()); // trailing delimiter
        outStream.close();
    }
}
