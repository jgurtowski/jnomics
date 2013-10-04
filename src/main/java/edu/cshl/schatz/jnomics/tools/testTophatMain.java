package edu.cshl.schatz.jnomics.tools;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class testTophatMain {
	public static void main(String[]  args) throws Exception{
		URI user_hdfs_path = null;
		try {
            Configuration conf = new Configuration();
            System.out.println(conf);
            System.out.println(conf.get("fs.default.name"));
            FileSystem fs = FileSystem.get(conf);
            System.out.println("URI is " + fs.getUri());
            user_hdfs_path = fs.getUri();
            //DistributedCache.addCacheArchive(new URI(user_hdfs_path+"user/sramakri/cufflinks.tar"), conf );
            //DistributedCache.addArchiveToClassPath(archive, conf, fs);
            //FileStatus[] stats = fs.listStatus(new Path(user_hdfs_path));
            System.out.println(System.getProperty("user.home"));  
            Path inpath = new Path(user_hdfs_path+"/user/sramakri/t1.1.fq");
            System.out.println("Inpath is "+ inpath);
            String inpathstr = inpath.toString();
            Path outpath = new Path("/bluearc/home/schatz/sramakri/hadoopfs/tophatout1");
            String outpathstr = outpath.toString(); 
            Path binary = new Path(user_hdfs_path+"/user/sramakri/tophat_v1.tar");
            String binarystr = binary.toString();
            Path pref_genome =  new Path(user_hdfs_path+"/user/sramakri/ecoli.fa");
            String ref_genome = pref_genome.toString();
            String ref = ref_genome.substring(ref_genome.lastIndexOf(".")+1);
            String cufflinks_opt = "-p 8";
            if(!fs.exists(inpath) || !fs.exists(binary) || !fs.exists(pref_genome)) {
            	return ;
            }else if(fs.exists(outpath)){
            	fs.delete(outpath);
            	System.out.println("Output path exists hence deleted");
            }
            fs.close();
            alignTophat tophat = new alignTophat();
            tophat.align(conf, inpathstr, cufflinks_opt , outpathstr, ref_genome,binarystr);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
