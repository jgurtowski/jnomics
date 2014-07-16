package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.slf4j.LoggerFactory;

import us.kbase.shock.client.BasicShockClient;
import edu.cshl.schatz.jnomics.grid.JnomicsGridJobBuilder;
import edu.cshl.schatz.jnomics.util.FileUtil;
import edu.cshl.schatz.jnomics.util.ShockUtil;

import java.io.FilenameFilter;
import java.io.File;
import java.io.InputStream;

public class GridJobMain extends Configured implements Tool {

	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(JnomicsGridJobBuilder.class);

	private static String printUsage = "Invalid Argument!  Program will now Exit... ";

	private static final Map<String,Class> gridClasses =
			new HashMap<String,Class>(){
		{
			put("tophat",AlignTophat.class);
			put("Cufflinks",CufflinksSuite.class);
		}
	};

	public static void main(String[]  args) throws Exception{

		String jobname = args[0].substring(args[0].lastIndexOf(":") + 1);
		final String jobid = System.getenv("JOB_ID");
		String userhome = System.getProperty("user.home");
		Configuration conf = new Configuration();
		File conffile = new File(System.getProperty("user.home")+"/" + jobname + ".xml");
		logger.info(" args is " + args[0] +" jobname is  " + jobname );
		if (conffile.canRead()){
			logger.info("Reading the Job Configuration File:  " + conffile);
		}

		FileSystem fs1 = null;
		conf.addResource(new Path(conffile.getAbsolutePath()));
		String gridJob = conf.get("grid.job.name");
                System.out.println(gridJob);
		String[] jobparts =  conf.get("grid.job.name").split("-");
		String username = jobparts[0];
		Path hdfs_job_path = null;

		try{
			URI hdfs_uri = new URI(conf.get("fs.default.name"));
			fs1 = FileSystem.get(hdfs_uri,conf,username);
			hdfs_job_path = new Path( fs1.getHomeDirectory().toString());
                        logger.info("hello");
			if(gridJob.matches(".*-fastqtopege-.*")){
                            String in1 = conf.get("fastq1","");
                            String in2 = conf.get("fastq2","");
                            String dest = conf.get("outfile", "");
                            if( in1 == "" || in2 == "" || dest == ""){
                                throw new Exception("bad configuration");
                            }
                            Path in1p = new Path(in1);
                            Path in2p = new Path(in2);
                            Path destp = new Path(dest);
                            InputStream in1fs = fs1.open(in1p);
                            InputStream in2fs = fs1.open(in2p);
                            new PairedEndLoader().load(in1fs, in2fs, destp, fs1);
                            in1fs.close();
                            in2fs.close();
                        }else if(gridJob.matches(".*-write-.*")){
				String shockurl = conf.get("shock-url","");
				String shocktoken = new String(Base64.decodeBase64(conf.get("shock-token","")));
				String proxy = conf.get("http-proxy","");
				//ShockUtil.setHttpProxy(proxy);
				logger.info("Shock url " + shockurl + " Token is  " + shocktoken);
				String filename = conf.get("grid.input.dir","");
				logger.info("filename " + filename);
				try{
					FileUtil.copyToShock(shockurl, shocktoken,proxy, fs1 , filename);
					copyanddelete(fs1,jobid,userhome,hdfs_job_path);
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if(gridJob.matches(".*-read-.*")){
				String shockurl = conf.get("shock-url","");
				String shocktoken = new String(Base64.decodeBase64(conf.get("shock-token","")));
				logger.info("Shock url " + shockurl + " Token is  " + shocktoken);
				String proxy = conf.get("http-proxy","");
				//ShockUtil.setHttpProxy(proxy);
				String nodeid = conf.get("grid.input.dir","");
				String dest = conf.get("grid.output.dir","");
				logger.info("nodeid : " + nodeid);
				try{
					FileUtil.copyFromShock(shockurl, shocktoken, proxy, fs1 , nodeid,dest);
//					copyanddelete(fs1,jobid,userhome,hdfs_job_path);
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if(gridJob.matches(".*-wsupload-.*")){
				System.out.println("Uploading the Expression object to Workspace");
				WorkspaceUpload ws = new WorkspaceUpload(conf);
				ws.uploadExpression(fs1,conf);
//				copyanddelete(fs1,jobid,userhome,hdfs_job_path);
			}else if(gridJob.matches(".*-tophat-.*")){
				System.out.println("Executing  tophat");
				AlignTophat tophat = new AlignTophat(conf);
				tophat.prepareBinaries(fs1, conf);
				tophat.align(fs1, conf);
//				copyanddelete(fs1,jobid,userhome,hdfs_job_path);
			}else { 
				System.out.println("Executing Cufflinks");
				CufflinksSuite cuff = new CufflinksSuite(conf);
				cuff.prepareBinaries(fs1, conf);
				if(gridJob.matches(".*-cufflinks-.*")){
					cuff.callCufflinks(fs1,conf);
//					copyanddelete(fs1,jobid,userhome,hdfs_job_path);
				}else if(gridJob.matches(".*-cuffmerge-.*")){
					cuff.callCuffmerge(fs1,conf);
//					copyanddelete(fs1,jobid,userhome,hdfs_job_path);
				}else if(gridJob.matches(".*-cuffdiff-.*")){
					cuff.callCuffdiff(fs1,conf);
//					copyanddelete(fs1,jobid,userhome,hdfs_job_path);
				}else if(gridJob.matches(".*-cuffcompare-.*")){
					cuff.callCuffcompare(fs1,conf);
					
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
//			throw new Exception(e.toString());
		}finally{
                    //copyanddelete(fs1,jobid,userhome,hdfs_job_path);
			 		copyanddelete(fs1,jobid,userhome,hdfs_job_path);
                    fs1.close();
                    conffile.delete();
                   
		}
	}

	private static WorkspaceUpload WorkspaceUpload(Configuration conf) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {

		return 0;
	}

	public static void copyanddelete(FileSystem fs2,final String job_id, String source, Path hdfs_job_path ) throws Exception {

		File[] files = new File(source).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(job_id);
			}
		});
		for(File file : files)
		{
			String fname = file.getName();
			logger.info("Copying log files to hdfs  : " +  hdfs_job_path+"/"+ file.getName());
			//        if(!fname.endsWith("po"+job_id) && !fname.endsWith("pe"+job_id)){
			fs2.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path);
			//	}else{
			//	 logger.info("Deleting file" + file.getAbsolutePath() ); 
			//	 file.delete();
			//	}
		}

	}
}
