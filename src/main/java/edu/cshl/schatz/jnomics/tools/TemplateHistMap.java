package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.ob.writable.BinWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: james
*/
public class TemplateHistMap extends JnomicsMapper<SAMRecordWritable,NullWritable,BinWritable,IntWritable> {

    private static final String DEFAULT_BINSIZE = "10";
    private static final String DEFAULT_MINBIN = "0";
    private static final String DEFAULT_MAXBIN = "2000";
    
    private static final JnomicsArgument binsize_arg = new JnomicsArgument("binsize",
            "binsize for histogram ["+DEFAULT_BINSIZE+"]",false);
    private static final JnomicsArgument maxbin_arg = new JnomicsArgument("maxbin",
            "Value for minimum bin for histogram ["+DEFAULT_MAXBIN+"]",false);
    private static final JnomicsArgument minbin_arg = new JnomicsArgument("minbin",
            "Value for maximum bin for histogram ["+DEFAULT_MINBIN+"]",false);


    private BinWritable bin;
    private final IntWritable count = new IntWritable(1);

    private int binsize,minbin,maxbin;
    
    @Override
    public Class getOutputKeyClass() {
        return BinWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return IntWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{binsize_arg,maxbin_arg,minbin_arg};
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return TemplateHistReduce.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        binsize = Integer.parseInt(conf.get(binsize_arg.getName(),DEFAULT_BINSIZE));
        minbin = Integer.parseInt(conf.get(minbin_arg.getName(), DEFAULT_MINBIN));
        maxbin = Integer.parseInt(conf.get(maxbin_arg.getName(), DEFAULT_MAXBIN));
        bin = new BinWritable(binsize);
    }

    @Override
    protected void map(SAMRecordWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

        //mates do not align to same chromosome
        if(key.getMateReferenceName().toString().compareTo("=") != 0 &&
                key.getMateReferenceName().compareTo(key.getReferenceName()) !=0 )
            return;


        //one or both of the mates are unmapped
        if(key.getReferenceName().toString().compareTo("*") == 0 ||
                key.getMateReferenceName().toString().compareTo("*") == 0){
            return;
        }

        int template_size = 0;

        int read_len = key.getReadString().toString().length();
        int start = key.getAlignmentStart().get();
        int mstart = key.getMateAlignmentStart().get();

        if(start > mstart){ // we are to the right of the mate
            template_size = start + read_len - mstart;
        }else{
            return;
        }

        if(template_size > maxbin){
            template_size = maxbin;
        }else if(template_size < minbin){
            template_size = minbin;
        }

        bin.set(template_size / binsize);

        context.write(bin,count);
    }

}
