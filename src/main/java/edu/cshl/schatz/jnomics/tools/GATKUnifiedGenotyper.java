package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * User: james
 */
public class GATKUnifiedGenotyper extends GATKBaseReduce<NullWritable,NullWritable>{

    private FileSystem fs;
    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    /**
     * We'll manually put VCF files into HDFS no need for part files
     * @return NullOuputFormat
     */
    /*@Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return NullOutputFormat.class
        }*/

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
            throws IOException, InterruptedException {

        super.reduce(key, values, context);

        File recalBam = getRecalBam();
        File recalFile = getRecalFile();

        /** Unified Genotyper **/
        System.out.println("Unified Genotyper");
        String region = key.getRef().toString()+":"+startRange+"-"+endRange;
        File region_vcf = new File(key.getRef() + "-"+key.getBin()+".vcf");
        String ug_cmd = String.format("java -Xmx%dm -jar %s -T UnifiedGenotyper -R %s -I %s -glm BOTH -L \'%s\' -o \'%s\' -BQSR %s -stand_call_conf 50 -stand_emit_conf 10.0 -minIndelCnt 5 -indelHeterozygosity 0.0001",
                gatk_jvm_mem, gatk_binary, reference_fa, recalBam,region,region_vcf,recalFile
        );
        System.out.println(ug_cmd);
        ProcessUtil.runCommandEZ(new Command(ug_cmd));
        context.progress();
        markForDeletion(region_vcf);

        /** Copy our file back to hdfs**/
        //Path hdfs_vcf = FileOutputFormat.getPathForWorkFile(context,region_vcf.toString(),"");
        Path outdir = FileOutputFormat.getOutputPath(context);
        fs.copyFromLocalFile(new Path(region_vcf.getAbsolutePath().toString()),outdir);
        context.progress();
    }
}



