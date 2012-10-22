package edu.cshl.schatz.jnomics.ob.writable;

import org.apache.hadoop.io.ArrayWritable;

/**
 * User: james
 */
public class StringArrayWritable extends ArrayWritable {
    public StringArrayWritable(){
        super(new String[]{});
    }
 
    public StringArrayWritable(String []arr){
        super(arr);
    }
}