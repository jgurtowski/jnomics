package edu.cshl.schatz.jnomics.tools;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.*;


public class ExpressionLevel {

	//private String feature;
	//private long startpt;
	//private long endpt;
	//private String fpkm;
	
	private Map<String, Double> exprlevelMap;
	
//	public ExpressionLevel(String feature,long startpt, long endpt, String fpkm){
//		this.feature=  feature;
//		this.startpt = startpt;
//		this.endpt = endpt;
//		this.fpkm = fpkm;
//	}
	
	public Map<String,Double> getExprlevel() {
        return exprlevelMap;
    }
//    public void setFeature(String feature) {
//        this.feature = feature;
//    }
//    public long getStart() {
//        return startpt;
//    }
//    public void setStart(long startpt) {
//        this.startpt = startpt;
//    }
//    public long getEndpt() {
//        return endpt;
//    }
//    public void setEndpt(long endpt) {
//        this.endpt = endpt;
//    }
//    public String getFpkm() {
//        return fpkm;
//    }
//    public void setFpkm(String fpkm) {
//        this.fpkm = fpkm;
//    }
     
//    public Map<String, String>  map(String feature, long start, long end, String fpkm){
//    	
//    	Map<String, String> exprlevelMap = new HashMap<String, String>();
//    	exprlevelMap.put(feature + ":" + start + ":" + end,fpkm);
//    	return exprlevelMap;
//    }
    
    public void setExprlevel(InputStream in){
    	BufferedReader bfr = new BufferedReader(new InputStreamReader(in));
    	String line;
    	String feature , startpt, endpt , value; 
    	Double fpkm;
    	exprlevelMap = new HashMap<String, Double>();
    	try {
			while((line = bfr.readLine()) != null){
				String[] columns = line.split("\t" ,-1);
				feature = columns[2];
				startpt = columns[3];
				endpt = columns[4];
			    value = columns[8];
			    String[] attr = value.split(" ");
			    fpkm = Double.parseDouble(attr[7].replaceAll("[\";]", ""));
				exprlevelMap.put(feature + ":" + startpt + ":" + endpt,fpkm);
			}
	   } catch (IOException e) {
			
			e.printStackTrace();
	   }
    	//this.exprlevelMap = exprlevelMap;
    }
    
}
