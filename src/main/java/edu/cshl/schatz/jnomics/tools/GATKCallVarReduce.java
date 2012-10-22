package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 */

public class GATKCallVarReduce extends GATKBaseReduce<NullWritable,NullWritable>{

    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }


    @Override
    public Map<String,String> getConfModifiers(){
        return new HashMap<String, String>(){
            {
                put("mapred.reduce.tasks.speculative.execution","false");
            }
        };
    }




    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
       throws IOException, InterruptedException {

        super.reduce(key,values,context);

        /**Call Variations**/
        File recal_vcf = new File(key.getRef()+"-"+key.getBin()+".vcf");
        recal_vcf.createNewFile();

        String variation_cmd = String.format("java -Xmx3g -jar %s -T UnifiedGenotyper -L %s:%d-%d -R %s -I %s -o %s -stand_call_conf 50 -stand_emit_conf 10.0 -minIndelCnt 5 -indelHeterozygosity 0.0001",
                gatk_binary,key.getRef(),startRange,endRange,reference_fa,tmpBam,recal_vcf);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(variation_cmd));

        tmpBam.delete();

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(recal_vcf.toString()), new Path(conf.get("mapred.output.dir")+"/"+recal_vcf));
    }
}
