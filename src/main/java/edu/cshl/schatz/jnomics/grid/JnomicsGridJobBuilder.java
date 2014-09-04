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
import edu.cshl.schatz.jnomics.util.FileUtil;

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


class Lock{

	  private  boolean  isLocked = false;

	  public synchronized   void lock()
	  throws InterruptedException{
	    while(isLocked){
	       wait();
	    }
	    isLocked = true;
	  }

	  public synchronized  void unlock(){
	    isLocked = false;
	    notify();
	  }
	}

public class JnomicsGridJobBuilder {

	private static Lock lock = new Lock();
	private final org.slf4j.Logger logger = LoggerFactory.getLogger(JnomicsGridJobBuilder.class);

	private Configuration conf = null;
	List<String> archives = new ArrayList<String>();
	public static Map<Integer,String> returnCode =  new HashMap<Integer, String>();
//	private String token = null;

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
	
//	public JnomicsGridJobBuilder(Configuration conf,String token){
//		this.conf = conf;
//		this.token = token;
//
//	}
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
		lock.lock();
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
			jt.setNativeSpecification("-j y");
			jt.setJobName(jobname);
			jobId = session.runJob(jt);
			System.out.println("Jobname is : " + jt.getJobName());
			//scontact = session.getContact();
			session.deleteJobTemplate(jt);
		}catch(Exception e){
			e.printStackTrace();
//			throw new Exception(e.getMessage());
		}finally{
			session.exit();
			lock.unlock();
		}
		//conf.set("grid_session",scontact);
		conf.set("grid_jobId",jobId);
		return this ;
	}

	public  String getjobstatus(String JobId)throws Exception{
		lock.lock();
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		int ret = 0;
		String jobinfo;
		try {
			session.init("");
			ret  = session.getJobProgramStatus(JobId);
			//session.exit();
		} catch (DrmaaException e) {
			BufferedReader stdInput = null;
			BufferedReader stdError = null;
			try{
				/* I want to run the command qcct -j jobid | grep exit. It that gives a exit status 1 , I want the thread to sleep and retry 2 times. */
				String str = null;
				Thread.sleep(1000);
				String[] cmd = {"/bin/sh", "-c","qacct -j "+ JobId.trim() + " | grep exit"};
//				String cmd = "qacct -j "+ JobId.trim() + " | grep exit";
//			        int jobstat = ProcessUtil.runCommand(new Command(cmd));
//				if(jobstat == 0){
//					ret = 48;
//				}else{
//				    ret = 64;	
//				}
				/* old code for grid-job-status */
				Process p = Runtime.getRuntime().exec(cmd);
				p.waitFor();
				stdInput = new BufferedReader(new 
							InputStreamReader(p.getInputStream()));
				stdError = new BufferedReader(new 
							InputStreamReader(p.getErrorStream()));
				if((str = stdInput.readLine()) != null){
					jobinfo = str;
					String[] jobstatus = jobinfo.split("  ");
					if(jobstatus[1].trim().equals("0")){
						ret = 48;
					}else if(jobstatus[1].trim().equals("1")){
						ret = 64;
					}
				}
				stdInput.close();
				stdError.close();
				//else{
				//	 	Thread.sleep(1000);
					 //	#### Retry to run the command for 3 times and return a failure code.
				//	 }

				}catch(Exception ee){
					ee.printStackTrace();
				}
		}finally{
	        session.exit();
	        lock.unlock();

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
