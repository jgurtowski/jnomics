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

import net.sf.samtools.SAMProgramRecord;

/**
 * The Jnomics extension of the <a
 * href="http://picard.sourceforge.net">Picard's</a> SAM/BAM-specific
 * {@link SAMProgramRecord} class. Currently, little functionality is added in
 * the extension, but in the future general-purpose methods will be added here.
 * 
 * @see <a href="http://picard.sourceforge.net">Picard</a>
 * @author Matthew A. Titmus
 */
public class ProgramRecord extends SAMProgramRecord {
    /**
     * @param srcProgramRecord The program record to clone.
     */
    public ProgramRecord(SAMProgramRecord srcProgramRecord) {
        // Sets group id and attributes
        super(srcProgramRecord.getProgramGroupId(), srcProgramRecord);

        setCommandLine(srcProgramRecord.getCommandLine());
        setPreviousProgramGroupId(srcProgramRecord.getPreviousProgramGroupId());
        setProgramName(srcProgramRecord.getProgramName());
        setProgramVersion(srcProgramRecord.getProgramVersion());
    }

    /**
     * @param programGroupId
     */
    public ProgramRecord(String programGroupId) {
        super(programGroupId);
    }

    /**
     * @param id
     * @param srcProgramRecord
     */
    public ProgramRecord(String id, SAMProgramRecord srcProgramRecord) {
        super(id, srcProgramRecord);
    }
}
