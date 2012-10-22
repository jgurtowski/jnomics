package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KCounterReduce extends JnomicsReducer <FixedKmerWritable, LongWritable, FixedKmerWritable, LongWritable> {

    private long totalCount;

    private final LongWritable totalCountWritable = new LongWritable();

    public static class KCountParitioner extends Partitioner<FixedKmerWritable,LongWritable>{

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
        public int getPartition(FixedKmerWritable fixedKmerWritable, LongWritable intWritable, int i) {
            long partition_size  = ((long) Math.pow(5,fixedKmerWritable.getKsize())) /  i + 1;
            byte []b = fixedKmerWritable.get();
            long kval = 0;
            for(int j=b.length-1; j>= 0; j--){
                kval += byteMap.get(new Character((char) b[j])) * Math.pow(5, b.length-1-j);
            }
            return (int) (kval / partition_size);
        }
    };

    @Override
    public Class getOutputKeyClass() {
        return FixedKmerWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return KCountParitioner.class;
    }

    @Override
    protected void reduce(FixedKmerWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        totalCount = 0;
        for(LongWritable count: values){
            totalCount += count.get();
        }
        totalCountWritable.set(totalCount);
        context.write(key,totalCountWritable);
    }
}
