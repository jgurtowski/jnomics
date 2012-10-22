/*
 * This file is part of Jnomics.
 * Copyright 2011 Matthew A. Titmus
 * All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *       
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *       
 *     * Neither the name of the Cold Spring Harbor Laboratory nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.cshl.schatz.jnomics.ob.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMValidationError;

import org.apache.hadoop.fs.Path;

/**
 * The Jnomics extension of the <a
 * href="http://picard.sourceforge.net">Picard's</a> SAM/BAM-specific
 * {@link SAMFileHeader} class. Currently, little functionality is added in the
 * extension, but in the future general-purpose methods will be added here.
 * 
 * @see <a href="http://picard.sourceforge.net">Picard</a>
 * @author Matthew A. Titmus
 */
public class HeaderData extends SAMFileHeader {
    private Path filePath = null;
    private String id;

    public HeaderData() {
        super();
    }

    public HeaderData(SAMFileHeader toClone) {
        if (toClone instanceof HeaderData) {
            setId(toClone.getId());
        }

        for (Map.Entry<String, String> e : toClone.getAttributes()) {
            setAttribute(e.getKey(), e.getValue());
        }

        setComments(new ArrayList<String>(toClone.getComments()));
        setGroupOrder(toClone.getGroupOrder());
        setProgramRecords(new ArrayList<SAMProgramRecord>(toClone.getProgramRecords()));
        setReadGroups(new ArrayList<SAMReadGroupRecord>(toClone.getReadGroups()));
        setSortOrder(toClone.getSortOrder());
        setTextHeader(toClone.getTextHeader());
        setValidationErrors(new ArrayList<SAMValidationError>(toClone.getValidationErrors()));

        List<SAMSequenceRecord> src, dest;
        src = toClone.getSequenceDictionary().getSequences();
        dest = new ArrayList<SAMSequenceRecord>(src.size());

        for (SAMSequenceRecord r : src) {
            dest.add(new ReferenceSequenceRecord(r));
        }

        setSequenceDictionary(new SAMSequenceDictionary(dest));
    }

    /**
     * @return The filePath
     */
    public Path getFilePath() {
        return filePath;
    }

    /*
     * @see net.sf.samtools.AbstractSAMHeaderRecord#getId()
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * @param filePath The filePath to set
     */
    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    /**
     * @param id The id to set
     */
    public void setId(String id) {
        this.id = id;
    }
}
