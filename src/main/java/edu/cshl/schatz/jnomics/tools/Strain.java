package edu.cshl.schatz.jnomics.tools;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Strain {
	
// 
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;

	@JsonProperty("genome_id")
	private String genome_id ;
	
	@JsonProperty("reference_strain")
	private String reference_strain;
	
	@JsonProperty("wild_type")
	private String wild_type;
	
	@JsonProperty("description")
	private String description;
	
	@JsonProperty("name")
	private String name;
	

	public String getGenomeId() {
        return genome_id;
    }
    public void setGenomeId(String genome_id) {
        this.genome_id = genome_id;
    }

	public String getRefStrain() {
        return reference_strain;
    }
    public void setRefStrain(String ref_strain) {
        this.reference_strain = ref_strain;
    }

	public String getWildType() {
        return wild_type;
    }
    public void setWildType(String wild_type) {
        this.wild_type = wild_type;
    }

	public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }

	public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
}
