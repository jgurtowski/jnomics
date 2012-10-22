package edu.cshl.schatz.jnomics.ob.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: james
 */
public class BinWritable implements WritableComparable {

    IntWritable binsize;
    IntWritable bin;
    
    public BinWritable(){
        binsize = new IntWritable();        
        bin = new IntWritable();
    }

    public BinWritable(int binsize){
        this.binsize = new IntWritable(binsize);
        this.bin = new IntWritable();
    }
    
    public void set(int bin){
        this.bin.set(bin);
    }
    
    public void set(IntWritable binw){
        this.bin.set(binw.get());
    }

    public IntWritable getBin(){
        return bin;
    }
    
    @Override
    public int compareTo(Object o) {
        BinWritable other = (BinWritable)o;
        return this.bin.compareTo(other.getBin());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        binsize.write(dataOutput);
        bin.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        binsize.readFields(dataInput);
        bin.readFields(dataInput);
    }
    
    public String toString(){
        return Integer.toString(this.bin.get() * this.binsize.get());
    }
}
