package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 */
public class TextKmerCountReduce extends JnomicsReducer<Text,LongWritable, Text, LongWritable>{

    private long count;
    
    private LongWritable longWritable = new LongWritable();


    public static class TextKmerCountParitioner extends Partitioner<Text,LongWritable> {

        private static final Map<Character, Integer> byteMap = new HashMap<Character, Integer>(){
            {
                put('A', 0);
                put('C', 1);
                put('G', 2);
                put('N', 3);
                put('T', 4);
            }
        };

        @Override
        public int getPartition(Text textKmer, LongWritable longWritable, int i) {
            String kmer = textKmer.toString();
            long partition_size  = ((long) Math.pow(5,kmer.length())) /  i + 1;
            byte []b = kmer.getBytes();
            long kval = 0;
            for(int j=b.length-1; j>= 0; j--){
                kval += byteMap.get(new Character((char) b[j])) * Math.pow(5, b.length-1-j);
            }
            return (int) (kval / partition_size);
        }
    };

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return TextKmerCountParitioner.class;
    }

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return TextOutputFormat.class;
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        count = 0;
        for(LongWritable lw : values){
            count += lw.get();
        }
        longWritable.set(count);
        context.write(key,longWritable);
    }
}
