package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CufflinksMapMain {
	public static void main(String[]  args) throws Exception{
		URI user_hdfs_path = null;
		FileSystem fs = null;
		try {
            Configuration conf = new Configuration();
            System.out.println(conf.get("fs.default.name"));
            fs = FileSystem.get(conf);
            System.out.println(conf.get("shock-url"));
            System.out.println("URI is " + fs.getUri());
            user_hdfs_path = fs.getUri(); 
            
            /* Cufflinks input */
            Path inpath = new Path(user_hdfs_path+"/user/sramakri/tophatfiles/accepted_hits2.bam");
            String inpathstr = inpath.toString();
            Path outpath = new Path("/bluearc/home/schatz/sramakri/hadoopfs/c1_t2out");
            String outpathstr = outpath.toString(); 
            Path binary = new Path(user_hdfs_path+"/user/sramakri/cufflinks.tar");
            String binarystr = binary.toString();
            String cufflinks_opt = "-p 8";
            if(!fs.exists(inpath) || !fs.exists(binary)) {
            	return ;
            }else if(fs.exists(outpath)){
            	fs.delete(outpath);
            	System.out.println("Output path exists hence deleted");
            }
            /* Cuff merge  input*/
//            ArrayList<String> cuffmerge_input = new ArrayList<String>();
//            cuffmerge_input.add("/bluearc/home/schatz/sramakri/hadoopfs/c1_t1out/transcripts.gtf");
//            cuffmerge_input.add("/bluearc/home/schatz/sramakri/hadoopfs/c1_t2out/transcripts.gtf");
//            File merge_input = new File("/bluearc/home/schatz/sramakri/hadoopfs/assemblies.txt");
//			PrintWriter pw = new PrintWriter(new FileOutputStream(merge_input));
//            for( String mfile : cuffmerge_input) {
//            	pw.println(mfile);
//            }
            ArrayList<String> cuffdiff_bam_input = new ArrayList<String>();
            cuffdiff_bam_input.add("/bluearc/home/schatz/sramakri/hadoopfs/accepted_hits.bam");
            cuffdiff_bam_input.add("/bluearc/home/schatz/sramakri/hadoopfs/accepted_hits2.bam");
            CufflinksMap1 cuff = new CufflinksMap1();
           // cuff.Preparebinaries(fs, binarystr);
//           cuff.callCufflinks(fs, inpathstr, cufflinks_opt , outpathstr);
//            cuff.callCuffmerge(fs,merge_input.getAbsolutePath(),cufflinks_opt,"/bluearc/home/schatz/sramakri/hadoopfs/mergedout");
           // cuff.callCuffmerge(fs,"/bluearc/home/schatz/sramakri/hadoopfs/assemblies.txt",null,cufflinks_opt,"/bluearc/home/schatz/sramakri/hadoopfs/mergedout");
           cuff.callCuffdiff(fs, cuffdiff_bam_input,"/bluearc/home/schatz/sramakri/hadoopfs/ecoli.fa", "/bluearc/home/schatz/sramakri/hadoopfs/mergedout/merged.gtf","-p 8 -L C1,C2", "/bluearc/home/schatz/sramakri/hadoopfs/diffout");
		}catch(Exception e){
			e.printStackTrace();
		}finally {
		    fs.close();
		}
	}
}
