package edu.cshl.schatz.jnomics.tools;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Protocol {

//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;

	@JsonProperty("name")
	private String name;
	
	@JsonProperty("description")
	private String description;

	
	public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
  

}
