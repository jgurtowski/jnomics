package edu.cshl.schatz.jnomics.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import us.kbase.cdmi_api.CDMI_API;
import us.kbase.cdmi_api.region_of_dna;

public class GetGenomeFeatures{

	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(GetGenomeFeatures.class);

	public String getfeatures (FileSystem fs, String cdmi_url, List<String> genome_id ){
		logger.info("Getting Kbase features for genome");
		CDMI_API cdmi;
		String id;
		FSDataOutputStream fsout =  null;
		Map<String, List<String>> gfids = null ;
		List<String> fids = null ;
		List<String> ftype = new ArrayList<String>();
		Map<String,List<region_of_dna>> fids_to_locs;
		//		FSDataOutputStream fsout = null ;
		ftype.add("CDS");
		String name  = genome_id.get(0);
		String outfilename = "kb_"+name.substring(name.lastIndexOf("|")+ 1)+"_fids.txt";
		try {
			fsout = fs.create(new Path(outfilename));
			cdmi = new CDMI_API(cdmi_url);
			gfids = cdmi.genomes_to_fids(genome_id, ftype);
			for( Map.Entry<String,List<String>> fid : gfids.entrySet()) {
				id = fid.getKey();
				fids = fid.getValue();
			}
			fids_to_locs = cdmi.fids_to_locations(fids);
			for(Map.Entry<String,List<region_of_dna>> j : fids_to_locs.entrySet()) {
				String feat  = j.getKey();
				List<region_of_dna> locs = j.getValue();
				for(region_of_dna k : locs) {
					fsout.writeBytes(k.e_1.toString() + "\t" + k.e_2 + "\t" + (k.e_2 + k.e_4) +"\t" +k.e_3 +"\t" + feat + "\n");

				}
			}
		}catch(Exception e){
			e.toString();
		}finally{
			try {
				fsout.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return outfilename;

	}
}
