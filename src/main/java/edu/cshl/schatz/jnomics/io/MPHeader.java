package edu.cshl.schatz.jnomics.io;

import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.msgpack.annotation.Message;

import java.util.HashMap;

/**
 * User: james
 */

@Message
public class MPHeader {

    public static enum Codec{
        NONE,
        SNAPPY
    }
    
    
    public String delimiter;
    public String compression_codec;
    public HashMap<String,String> userdata;

}
