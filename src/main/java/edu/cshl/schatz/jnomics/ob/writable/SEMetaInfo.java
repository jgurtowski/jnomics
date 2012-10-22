package edu.cshl.schatz.jnomics.ob.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: james
 */
public class SEMetaInfo implements Writable{
    private Text file = new Text();
    private Text destination = new Text();

    public SEMetaInfo(String file, String destination) {
        this.file.set(file);
        this.destination.set(destination);
    }

    public SEMetaInfo() {
    }

    public String getFile() {
        return file.toString();
    }

    public void setFile(String file){
        this.file.set(file);
    }

    public String getDestination() {
        return destination.toString();
    }

    public void setDestination(String destination) {
        this.destination.set(destination);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        file.write(dataOutput);
        destination.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        file.readFields(dataInput);
        destination.readFields(dataInput);
    }
}