package edu.cshl.schatz.jnomics.ob;

import edu.cshl.schatz.jnomics.util.Nucleotide;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 *Deprecated, Do not use!
 *
 * Represent Kmers of a fixed length
 * Kmers are bitpacked and can only include ACGT. N's are converted to A's
 *
 */
public class FixedKmerWritable implements WritableComparable<FixedKmerWritable> {

    public static int MAX_KSIZE = 128;


    byte kmersize; //java doesn't have unsigned types so max ksize is 128 for now
    byte []kmer;

    public FixedKmerWritable(){}

    /*
    * All kmers must be the same size
    */
    public FixedKmerWritable(int ksize){
        assert ksize > 0 && ksize < FixedKmerWritable.MAX_KSIZE;
        this.kmersize = (byte)ksize;
        kmer = new byte[kmersize / 4 + 1]; //number of bytes necessary to hold kmer at 1:4 compression
    }

    public void setKsize(int k){
        kmersize = (byte) k;
    }

    public int getKsize(){
        return (int)kmersize;
    }
    
    /**
     *  zero the kmer
     */
    public void zero(){
        for(int i=0;i<kmer.length;i++)
            kmer[i] = 0;
    }


    /**
     * Set the kmer
     * Packs Kmer into smaller bit array for memory efficiency
     * Encoding works backwards so that bytes in the array
     * can be read from left to right. This is the opposite of how
     * most bitshift operations on integers work but is modeled after
     * the left to right 0->n indexing of array positions
     * @param a byte representation of kmer (from String.getBytes())
     * @throws Exception
     */
    public void set(byte[] a, int start, int end) throws Exception{
        assert a.length == kmersize;
        zero();

        for(int i=end-1; i>=start;i--){
            kmer[kmer.length-1] = (byte) ((kmer[kmer.length-1] & 0xff) >>> 2);
            for(int j=kmer.length-2; j>=0; j-- ){
                kmer[j+1] = (byte) ((kmer[j] << 6) | kmer[j+1]);
                kmer[j] = (byte) ((kmer[j] & 0xff) >>> 2);
            }
            if(!(a[i] == 'A' || a[i] == 'C' || a[i] == 'G' || a[i] == 'T' || a[i] == 'N')){
                throw new Exception("Unknown Character: " + a[i]);
            }
            kmer[0] = (byte) (kmer[0] | Nucleotide.charMap[a[i]]);
        }
    }             public void set(byte[]a) throws Exception{
        set(a,0,a.length);
    }

    public void set(String a) throws Exception{
        set(a.getBytes());
    }

    /**
     * gets the bytes back from the kmer
     * Slides a pair of 1 bits from left to right along the byte array
     * decoding the bits at that position using & operator. These are then
     * pushed into the returned byte array.
     * @return
     */
    public byte[] get(){
        byte[] charRep = new byte[kmersize];
        int t;
        int l=4;
        for(int i=0;i<kmer.length;i++){
            t = (3 << 6);
            if(i == kmer.length-1)
                l = kmersize % 4;
            for(int j = 0;j<l;j++){
                charRep[i*4+j] = (byte) Nucleotide.reverseCharMap[(kmer[i] & t) >>> (6-(2*j))];
                t >>>= 2;
            }
        }

        return charRep;
    }

    public byte[] getPackedBytes(){
        return kmer;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmersize = (byte) (in.readByte() & 0xff);
        kmer = new byte[kmersize / 4 + 1];
        in.readFully(kmer);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(kmersize & 0xff);
        out.write(kmer);
    }


    public String toString(){
        return new String(get());
    }

    @Override
    public int compareTo(FixedKmerWritable o) {
        byte[] okmer = o.getPackedBytes();
        int diff;
        for(int i=0;i< kmer.length; i++){
            if((diff = (kmer[i] & 0xff) - (okmer[i] & 0xff)) != 0)
                return diff;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != FixedKmerWritable.class)
            return false;
        byte[] o = ((FixedKmerWritable)obj).getPackedBytes();
        if(kmer.length != o.length)
            return false;

        for(int i=0;i<kmer.length;i++)
            if((kmer[i] & 0xff) - (o[i] & 0xff) != 0)
                return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(kmer);
    }

}
