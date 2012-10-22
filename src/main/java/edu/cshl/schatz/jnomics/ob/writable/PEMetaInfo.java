package edu.cshl.schatz.jnomics.ob.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: james
 */
public class PEMetaInfo implements Writable{
    private Text firstFile = new Text();
    private Text secondFile = new Text();
    private Text destination = new Text();

    public PEMetaInfo(String firstFile, String secondFile, String destination) {
        this.firstFile.set(firstFile);
        this.secondFile.set(secondFile);
        this.destination.set(destination);
    }

    public PEMetaInfo() {
    }

    public String getFirstFile() {
        return firstFile.toString();
    }

    public void setFirstFile(String firstFile) {
        this.firstFile.set(firstFile);
    }

    public String getSecondFile() {
        return secondFile.toString();
    }

    public void setSecondFile(String secondFile) {
        this.secondFile.set(secondFile);
    }

    public String getDestination() {
        return destination.toString();
    }

    public void setDestination(String destination) {
        this.destination.set(destination);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        firstFile.write(dataOutput);
        secondFile.write(dataOutput);
        destination.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstFile.readFields(dataInput);
        secondFile.readFields(dataInput);
        destination.readFields(dataInput);
    }
}