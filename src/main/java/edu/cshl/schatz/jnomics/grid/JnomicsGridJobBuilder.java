package edu.cshl.schatz.jnomics.grid;

import org.apache.hadoop.conf.Configuration;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.DefaultOutputStreamHandler;
import edu.cshl.schatz.jnomics.util.InputStreamHandler;
import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import java.util.Collections;


/**
 * User: Sri
 */
public class JnomicsGridJobBuilder {

	private final org.slf4j.Logger logger = LoggerFactory.getLogger(JnomicsGridJobBuilder.class);

	private Configuration conf = null;
	List<String> archives = new ArrayList<String>();
	public static Map<Integer,String> returnCode =  new HashMap<Integer, String>();

	public JnomicsGridJobBuilder(Configuration conf){
		this.conf = conf;
		returnCode.put(0, "UNDETERMINED");
		returnCode.put(16, "QUEUED_ACTIVE");
		returnCode.put(17, "SYSTEM_ON_HOLD");
		returnCode.put(18, "USER_ON_HOLD");
		returnCode.put(19, "USER_SYSTEM_ON_HOLD");
		returnCode.put(32, "RUNNING");
		returnCode.put(33, "SYSTEM_SUSPENDED");
		returnCode.put(34, "USER_SUSPENDED");
		returnCode.put(35, "USER_SYSTEM_SUSPENDED");
		returnCode.put(48, "DONE");
		returnCode.put(64, "FAILED");
	}

	public JnomicsGridJobBuilder setParam(String name, String value){	
		conf.set(name,value);
		return this;
	}
	public JnomicsGridJobBuilder setInputPath(String in){
		conf.set("grid.input.dir",in);
		return this;
	}

	public JnomicsGridJobBuilder setOutputPath(String out){
		conf.set("grid.output.dir", out);
		return this;
	}

	public JnomicsGridJobBuilder addConfModifiers(Map<String,String> modifiers){
		for(Map.Entry<String,String> entry: modifiers.entrySet()){
			conf.set(entry.getKey(),entry.getValue());
		}
		return this;
	} 

	public JnomicsGridJobBuilder setJobName(String name){
		conf.set("grid.job.name", name);
		return this;
	}  

	public JnomicsGridJobBuilder LaunchGridJob(Configuration conf) throws Exception{
		String scriptfile = new File(conf.get("grid-script-path")).getAbsolutePath();
		String workingdir = conf.get("grid_working_dir");
		String jobname = conf.get("grid.job.name");
		String slots = conf.get("grid-job-slots","1");
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		JobTemplate jt = null;
		String jobId = null;
		//String scontact = null;
		try {
			session.init("");
			jt = session.createJobTemplate();
			logger.info("Script file: " + scriptfile);
			jt.setRemoteCommand(scriptfile);
			jt.setArgs(Collections.singletonList(workingdir+":"+jobname));

			//jt.setArgs(Collections.singletonList("5"));
			//jt.setWorkingDirectory(workingdir);
			//jt.setErrorPath(":" + workingdir);
			//jt.setOutputPath(":" + workingdir);
			jt.setNativeSpecification("-pe threads "+ slots);
			jt.setJobName(jobname);
			jobId = session.runJob(jt);
			System.out.println("Jobname is : " + jt.getJobName());
			//scontact = session.getContact();
			session.deleteJobTemplate(jt);
		}catch(Exception e){
			throw new Exception();
		}finally{
			session.exit();
		}
		//conf.set("grid_session",scontact);
		conf.set("grid_jobId",jobId);
		return this ;
	}

	public String getjobstatus(String JobId)throws DrmaaException{
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		int ret = 0;
		String jobinfo;

		//		OutputStreamHandler jobinfo = null ;
		try {
			session.init("");
			//	System.out.println("contact is " + contact);
			//	session.init(contact);
			//	System.out.println(session.getContact());
			//	System.out.println("jobid " + JobId + " finished with exit status ");
			//	retval = session.wait(JobId,Session.TIMEOUT_NO_WAIT);
			//System.out.println("jobid " + JobId + " finished with exit status " +  retval.getExitStatus());

			ret  = session.getJobProgramStatus(JobId);
			//	System.out.println("values is " + Session.UNDETERMINED);
			//	retval = session.wait(JobId,Session.TIMEOUT_NO_WAIT);
			//	System.out.println("jobid " + JobId + " finished with exit status " +  retval.getExitStatus());

		} catch (DrmaaException e) {
			BufferedReader stdInput = null;
			BufferedReader stdError = null;
			try{
				String[] cmd = {"/bin/sh", "-c","qacct -j "+ JobId.trim() + " | grep exit"};
				Process p = Runtime.getRuntime().exec(cmd);
				p.waitFor();
				stdInput = new BufferedReader(new 
						InputStreamReader(p.getInputStream()));
				stdError = new BufferedReader(new 
						InputStreamReader(p.getErrorStream()));
				jobinfo = stdInput.readLine();
				System.out.println(jobinfo);
				String[] jobstatus = jobinfo.split("  ");
				System.out.println("String is " + jobstatus[1]);
				if(jobstatus[1].trim().equals("0")){
					ret = 48;
				}else if(jobstatus[1].trim().equals("1")){
					ret = 64;
				}
				stdInput.close();
				stdError.close();
				//			ProcessUtil.runCommand(new Command("qacct -j "+ JobId.trim(),new DefaultOutputStreamHandler()))
				//			System.out.println(jobinfo.toString());

			}catch(Exception ee){
				ee.printStackTrace();
			}
//			e.printStackTrace();

		}finally{
			session.exit();

		}
		return returnCode.get(ret);
	}

	private void ifNotSetConf(String key, String value){
		if(null == conf.get(key,null))
			conf.set(key,value);
	}

	public JnomicsGridJobBuilder setMaxSplitSize(int num){
		conf.setInt("mapred.max.split.size",num);
		return this;
	}

	public Configuration getJobConf() throws Exception {
		return this.conf;
	}


}
