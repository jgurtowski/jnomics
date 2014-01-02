package edu.cshl.schatz.jnomics.tools;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.*;

public class ExpressionSample {

		/**
	 * 
	 */
//	private static final long serialVersionUID = 1L;

		@JsonProperty("kb_id")
	 	private String kb_id;
		
		@JsonProperty("source_id")
	    private String source_id;
		
		@JsonProperty("type")
	    private String type;
	    
		@JsonProperty("numerical_interpretation")
		private String numerical_interpretation;
		
		@JsonProperty("description")
	    private String description;
		
		@JsonProperty("title")
	    private String title;
		
		@JsonProperty("data_quality_level")
	    private Integer data_quality_level;
		
		@JsonProperty("original_median")
	    private Float original_median;
		
		@JsonProperty("external_source_date")
	    private String external_source_date;
		
		@JsonProperty("data_expression_levels_for_sample_expression_levels")
	    private ExpressionLevel data_expression_levels_for_sample_expression_levels;
		
		@JsonProperty("genome_id")
	    private String genome_id;
		
		@JsonProperty("expression_ontology_terms")
	    private ExpressionOntologyTerm expression_ontology_terms;
		
		@JsonProperty("platform_id")
	    private String platform_id;

		@JsonProperty("default_control_sample")
		private String default_control_sample;
		
		@JsonProperty("averaged_from_samples")
	    private List<String> averaged_from_samples;
		
		@JsonProperty("protocol")
	    private Protocol protocol;
		
		@JsonProperty("strain")
	    private Strain strain;
		
		@JsonProperty("persons")
	    private Persons persons;
		
		@JsonProperty("molecule")
	    private String molecule;
		
		@JsonProperty("data_source")
	    private String data_source;
	     
		public String getKbId() {
	        return kb_id;
	    }
	    public void setKbId(String kb_id) {
	        this.kb_id = kb_id;
	    }
	    public String getSourceId() {
	        return source_id;
	    }
	    public void setSourceId(String source_id) {
	        this.source_id = source_id;
	    }
	    public String getSampleType() {
	        return type;
	    }
	    public void setSampleType(String sample_type) {
	        this.type = sample_type;
	    }
	    public String getInterpretation() {
	        return numerical_interpretation;
	    }
	    public void setInterpretation(String num_intpre) {
	        this.numerical_interpretation = num_intpre;
	    }
	    public String getDescription() {
	        return description;
	    }
	    public void setDescription(String description) {
	        this.description = description;
	    }
	    public String getTitle() {
	        return title;
	    }
	    public void setTitle(String title) {
	        this.title = title;
	    }
	    public int getDataQuality() {
	        return data_quality_level;
	    }
	    public void setDataQuality(Integer data_qual) {
	        this.data_quality_level = data_qual;
	    }
	    public float getOMedian() {
	        return original_median;
	    }
	    public void setOMedian(Float ori_med) {
	        this.original_median = ori_med;
	    }
	    public String getExtSrcDate() {
	        return external_source_date;
	    }
	    public void setExtSrcDate(String ext_src_date) {
	        this.external_source_date = ext_src_date;
	    }
	    public ExpressionLevel getExplevel(){
	    	return data_expression_levels_for_sample_expression_levels;
	    }
	    public void setExplevel(ExpressionLevel exprlevel ) {
	        this.data_expression_levels_for_sample_expression_levels = exprlevel;
	    }
	    public ExpressionOntologyTerm getExpOntology(){
	    	return expression_ontology_terms;
	    }
	    public void setExpOntology(ExpressionOntologyTerm exprOnto ) {
	        this.expression_ontology_terms = exprOnto;
	    }
	    public String getExpPlatId(){
	    	return platform_id;
	    }
	    public void setExpPlatId(String expr_plat_id ) {
	        this.platform_id = expr_plat_id;
	    }
	    public String getExpSampleId(){
	    	return default_control_sample;
	    }
	    public void setExpSampleId(String expr_sample_id ) {
	        this.default_control_sample = expr_sample_id;
	    }
	    public List<String> getExpSampleIds(){
	    	return averaged_from_samples;
	    }
	    public void setExpSampleIds(List<String> averaged_from_samples ) {
	        this.averaged_from_samples = averaged_from_samples;
	    }
	    public Protocol getProtocol(){
	    	return protocol;
	    }
	    public void setProtocol(Protocol protocol ) {
	        this.protocol = protocol;
	    }
	    public Strain getStrain(){
	    	return strain;
	    }
	    public void setStrain(Strain strain ) {
	        this.strain = strain;
	    }
	    public Persons getPersons(){
	    	return persons;
	    }
	    public void setPersons(Persons persons ) {
	        this.persons = persons;
	    }
	    public String getGenomeId(){
	    	return genome_id;
	    }
	    public void setGenomeId(String genome_id ) {
	        this.genome_id = genome_id;
	    }
	    public String getMolecule(){
	    	return molecule;
	    }
	    public void setMolecule(String molecule ) {
	        this.molecule = molecule;
	    }
	    public String getDataSource(){
	    	return data_source;
	    }
	    public void setDataSource(String data_source ) {
	        this.data_source = data_source;
	    }

//	    public static ExpressionSample createExprSample(String kb_id,InputStream in,String genome_id, String term_id,String term_def,String term_name,String Seq_type , String ref_genome) {
//			 
//	        ExpressionSample expsample = new ExpressionSample();
//	        expsample.setKbId(kb_id);
//	        expsample.setSourceId(kb_id);
//	        expsample.setSampleType("RNASeq");
//	        expsample.setInterpretation("FPKM");
//	        expsample.setDescription("");
//	        expsample.setTitle("");
//	        expsample.setDataQuality(0);
//	        expsample.setOMedian((float) 0);
//	        expsample.setExtSrcDate("");
//	        
//	        ExpressionLevel explvl = new ExpressionLevel();
//	        explvl.setExprlevel(in);
//	        expsample.setExplevel(explvl);
//	        
//	        expsample.setGenomeId(genome_id);
//	        
//	        ExpressionOntology expOnto = new ExpressionOntology();
//	        expOnto.setTermId(term_id);
//	        expOnto.setTermdef(term_def);
//	        expOnto.setTermName(term_name);
//	        expsample.setExpOntology(expOnto); 
//
//	        expsample.setExpPlatId(null);
//	        expsample.setExpSampleId(null);
//	        
//	        Protocol pro = new Protocol();
//	        pro.setName("RNA-Seq");
//	        pro.setDescription(Seq_type);
//	        expsample.setProtocol(pro);
//	        
//	        Strain str = new Strain();
//	        str.setGenomeId(genome_id);
//	        str.setDescription(null);
//	        str.setName(ref_genome);
//	        str.setRefStrain(ref_genome);
//	        str.setWildType(null);
//	        expsample.setStrain(str);
//	        
//	        expsample.setPersons(null);
//	        expsample.setMolecule(null);
//	        expsample.setDataSource(null);
//
//
//	        return expsample;
//	    }

}
