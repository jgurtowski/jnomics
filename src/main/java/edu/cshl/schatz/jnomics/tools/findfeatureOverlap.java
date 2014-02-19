package edu.cshl.schatz.jnomics.tools;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.FileUtil;
import edu.cshl.schatz.jnomics.util.ProcessUtil;

public class findfeatureOverlap {

	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(findfeatureOverlap.class);
	private static String workingdir ;

	/**
	 * <p>runbedtoolsScript</p>
	 * <pre>
	 * find the overlapping kbase CDS features with Cufflinks output
	 * </pre>
	 */
	public static void runbedtoolsScript(FileSystem fs,String scriptfile,String entityfile,Path dest){
		logger.info("Entering runbeltoolsScript");
		String gtfFile = "transcripts.gtf";
		int ret;
		try {
			workingdir = System.getProperty("user.dir");
			String cmd = String.format("%s %s/%s %s/%s",scriptfile,workingdir,entityfile,workingdir,gtfFile);
			logger.info("workingdir is " + workingdir);
			logger.info("cmd is " + cmd);
			ret  = ProcessUtil.runCommand(new Command(cmd));

			if(ret == 0 ){
				fs.copyFromLocalFile(false,new Path(workingdir+"/kbase_transcripts.gtf"),dest );
			}
			
			logger.info("Identified Overlapping featuress");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
