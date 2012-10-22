package edu.cshl.schatz.jnomics.ob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * User: james
 */
public class ReadCollectionWritable implements WritableComparable<ReadCollectionWritable>,
        SudoCollection<FastqStringProvider>, Iterable<ReadWritable>{

    private Text name;
    private List<ReadWritable> reads;

    public ReadCollectionWritable(){
        name = new Text();
        reads = new ArrayList<ReadWritable>();
    }

    public ReadCollectionWritable(ReadCollectionWritable other){
        this();
        name.set(other.getName());
        for(ReadWritable read:other.getReads()){
            reads.add(new ReadWritable(read));
        }
    }

    /**
     * compares name, description, sequences, quality in that order
     * @param o other ReadCollectionWritable
     * @return
     */
    @Override
    public int compareTo(ReadCollectionWritable o) {
        int diff;
        if((diff = name.compareTo(o.getName())) != 0) return diff;
        Iterator<ReadWritable> rit = reads.iterator();
        Iterator<ReadWritable> roit = o.getReads().iterator();
        while(rit.hasNext() && roit.hasNext()){
            if((diff = rit.next().compareTo(roit.next()) ) != 0) return diff;
        }

        return 0;
    }

    public int size(){
        return reads.size();
    }

    @Override
    public FastqStringProvider get(int idx) {
        return getRead(idx);
    }

    public ReadWritable getRead(int idx){
        return reads.get(idx);
    }
    
    public List<ReadWritable> getReads(){
        return reads;
    }

    public void addRead(ReadWritable read){
        reads.add(read);
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
        out.writeInt(reads.size());
        for(ReadWritable r : reads)
            r.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        reads.clear();
        int size = in.readInt();
        ReadWritable r;
        for(int i=0;i<size;i++){
            r = new ReadWritable();
            r.readFields(in);
            reads.add(r);
        }
    }

    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(name.toString());
        builder.append(":");
        builder.append(Arrays.toString(reads.toArray()));
        return builder.toString();
    }

    @Override
    public Iterator<ReadWritable> iterator() {
        return reads.iterator();
    }

    public void clear() {
        reads.clear();
    }
}

