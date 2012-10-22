package edu.cshl.schatz.jnomics.ob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * User: james
 */
public class AlignmentCollectionWritable implements WritableComparable<AlignmentCollectionWritable>,
        SudoCollection<FastqStringProvider>,Iterable<SAMRecordWritable>{

    private Text name;
    private List<SAMRecordWritable> alignments;

    public AlignmentCollectionWritable(){
        name = new Text();
        alignments = new ArrayList<SAMRecordWritable>();
    }

    /*public AlignmentCollectionWritable(AlignmentCollectionWritable other){
        this();
        name.set(other.getName());
        for(SAMRecordWritable alignment:other.getAlignments()){
            alignments.add(new SAMRecordWritable(alignment);
        }
    }*/

    /**
     * compares name, description, sequences, quality in that order
     * @param o other ReadCollectionWritable
     * @return
     */
    @Override
    public int compareTo(AlignmentCollectionWritable o) {
        int diff;
        if((diff = name.compareTo(o.getName())) != 0) return diff;
        Iterator<SAMRecordWritable> ait = alignments.iterator();
        Iterator<SAMRecordWritable> aoit = o.getAlignments().iterator();
        while(ait.hasNext() && aoit.hasNext()){
            if((diff = ait.next().compareTo(aoit.next()) ) != 0) return diff;
        }

        return 0;
    }
    
    public void clear(){
        alignments.clear();
    }
    
    public int size(){
        return alignments.size();
    }

    @Override
    public FastqStringProvider get(int idx) {
        return getAlignment(idx);
    }

    public SAMRecordWritable getAlignment(int idx){
        return alignments.get(idx);
    }
    
    public List<SAMRecordWritable> getAlignments(){
        return alignments;
    }

    public void addAlignment(SAMRecordWritable alignment){
        alignments.add(alignment);
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name.set(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        out.writeInt(alignments.size());
        for(SAMRecordWritable a : alignments)
            a.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        alignments.clear();
        int size = in.readInt();
        SAMRecordWritable a;
        for(int i=0;i<size;i++){
            a = new SAMRecordWritable();
            a.readFields(in);
            alignments.add(a);
        }
    }

    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(name.toString());
        builder.append(":");
        builder.append(Arrays.toString(alignments.toArray()));
        return builder.toString();
    }

    @Override
    public Iterator<SAMRecordWritable> iterator() {
        return alignments.iterator();
    }
}

