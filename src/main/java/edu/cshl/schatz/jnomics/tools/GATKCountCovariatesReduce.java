package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.File;
import java.io.IOException;

/**
 * User: james
 */
public class GATKCountCovariatesReduce extends GATKBaseReduce<NullWritable,NullWritable> {

    private JnomicsArgument vcf_mask_arg = new JnomicsArgument("vcf_mask","VCF file to mask known snps/indels",true);
    private File vcf_mask;
    
    private FileSystem fs;

    private Configuration conf;

    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        JnomicsArgument[] args = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[args.length+1];
        newArgs[0] = vcf_mask_arg;
        System.arraycopy(args,0,newArgs,1,args.length);
        return newArgs;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        fs = FileSystem.get(conf);
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> alignments, Context context)
            throws IOException, InterruptedException {
        
        super.reduce(key,alignments,context);

        vcf_mask = new File(conf.get(vcf_mask_arg.getName()));
        if(!vcf_mask.exists())
            throw new IOException("Could not find vcf_mask file:" + vcf_mask);

        /** Count Covars **/
        File recal_out = new File(key.getRef()+"-"+key.getBin()+".covar");
        String countCovars = String.format("java -Xmx3g -jar %s -T CountCovariates -L %s:%d-%d -R %s -I %s -knownSites %s -cov ReadGroupCovariate -cov QualityScoreCovariate -cov CycleCovariate -cov DinucCovariate -recalFile %s",
                gatk_binary,key.getRef(),startRange,endRange,reference_fa,tmpBam,vcf_mask,recal_out);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(countCovars));

        tmpBam.delete();
        fs.copyFromLocalFile(new Path(recal_out.toString()),new Path(conf.get("mapred.output.dir")));
        recal_out.delete();
    }
}
