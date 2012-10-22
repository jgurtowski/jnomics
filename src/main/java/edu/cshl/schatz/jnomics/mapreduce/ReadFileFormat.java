/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * Represents one of the standard short read sequence file formats.
 * 
 * @author Matthew Titmus
 */
public enum ReadFileFormat {
    BAM("bam"),
    BED("bed"),

    /**
     * FASTA format containing one or more short-read sequences. Not designed
     * for long sequences!
     * 
     * @see <a href="http://en.wikipedia.org/wiki/FASTA_format" />
     */
    FASTA("fa", "fasta", "fastn", "fna"),

    /**
     * FASTQ format.
     * <p>
     * Note: although some older FASTQ implementations allowed the sequence and
     * quality strings to be wrapped, this practice is not supported by Jnomics.
     * All entries in FASTQ-formatted input files must contain exactly 4 lines.
     * </p>
     * 
     * @see <a href="http://en.wikipedia.org/wiki/fastq" />
     */
    FASTQ("fq", "fastq"),

    /**
     * A flattened form of FASTQ containing 5 tab-delimited columns:
     * <ol>
     * <li>A '@' followed by a sequence identifier and an optional description
     * (like a FASTA title line). It does not contain a "/1" or "/2" suffix.
     * <li>Sequence 1 raw sequence letters.
     * <li>Sequence 1 encoded the quality values.
     * <li>Sequence 2 raw sequence letters.
     * <li>Sequence 2 encoded the quality values.
     * </ol>
     * Sequence quality values must contain the same number of symbols as
     * letters in the sequence. If converted from standard FASTQ files, any line
     * 3 comment values are discarded. See
     * http://en.wikipedia.org/wiki/FASTQ_format
     */
    FASTQ_FLAT("flq", "fastq-flat", "flat-fastq", "flastq", "pair"),

    /**
     * Unspecified or no format.
     */
    NONE(null, "", null, "none", "null"),

    /**
     * Sequence Alignment/Map format. See:
     * http://samtools.sourceforge.net/SAM1.pdf
     */
    SAM("sam");

    /**
     * A lookup map of name strings to read formats. Initialized by
     * {@link #init()} and accessible via {@link #get(String)}.
     */
    private transient static Map<String, ReadFileFormat> nameMap;

    /**
     * A set of all legal lookup names for the read format, including the
     * primary lookup name. Any of these are acceptable arguments for
     * {@link #get(String)}.
     */
    public final Set<String> allNames;

    /**
     * The file extension appended to files generated with this format.
     */
    public final String extension;

    /**
     * The official (primary) name of the read format, such that
     * <code>ReadFormat.get(someFormat.formatName) == someFormat</code> is
     * always <code>true</code>.
     */
    public final String name;

    /**
     * @param extensionAndName String to be used for both the primary read
     *            lookup name and file extension.
     */
    ReadFileFormat(String extensionAndName) {
        this(extensionAndName, extensionAndName);
    }

    /**
     * @param fileExtension The file extension used for files generated using
     *            this format.
     * @param lookupName The primary lookup name used to find this format.
     * @param additionalNames Additional lookup names for this format.
     */
    ReadFileFormat(String fileExtension, String lookupName, String... additionalNames) {
        Set<String> namesSet = new HashSet<String>(additionalNames.length + 3);

        namesSet.add(fileExtension);
        namesSet.add(lookupName);
        for (String altName : additionalNames) {
            namesSet.add(altName);
        }

        name = lookupName;
        allNames = Collections.unmodifiableSet(namesSet);
        extension = fileExtension;
    }

    /**
     * Attempts to determine format type from a file name (specifically, its
     * extension).
     * 
     * @param p The path to guess the read format for.
     * @return The enum associated with <code>name</code>, or <code>null</code>
     *         if no type can be determined.
     */
    public static ReadFileFormat get(Path p) {
        String name = p.getName();
        int i;

        if (-1 != (i = name.lastIndexOf('.'))) {
            name = name.substring(1 + i);
        }

        return get(name);
    }

    /**
     * An easy means to associate read names and respective enumerated values.
     * 
     * @param name A read format name (case insensitive).
     * @return The enum associated with <code>name</code>, or <code>null</code>
     *         if no such type exists.
     */
    public static ReadFileFormat get(String name) {
        if (nameMap == null) {
            init();
        }

        return nameMap.get(name != null ? name.toLowerCase() : null);
    }

    /**
     * Simply generates and returns a human readable list of format members,
     * intended for use in help menus.
     */
    public static String getAvailableFormatsString() {
        String formats = null;
        
        for (ReadFileFormat rf : ReadFileFormat.values()) {
            // Ignore the 'empty string' format.
            if (rf.toString().length() == 0) {
                continue;
            }
            
            if (formats == null) {
                formats = rf.toString().toLowerCase();
            } else {
                formats += ", " + rf.toString().toLowerCase();
            }
        }

        return formats;
    }

    public static void main(String[] args) throws Exception {
        for (ReadFileFormat rf : ReadFileFormat.values()) {
            System.out.println(rf.name);
            System.out.printf("\t%s%n", rf.extension);
            System.out.printf("\t%s%n", rf.allNames.toArray());
            System.out.printf("\tget(%s): %s%n", rf.name, get(rf.name));
        }
    }

    /**
     * Initializes the format lookup table.
     */
    private static void init() {
        nameMap = new HashMap<String, ReadFileFormat>();

        for (ReadFileFormat rf : ReadFileFormat.values()) {
            for (String s : rf.allNames) {
                nameMap.put(s, rf);
            }
        }
    }

    /**
     * Returns the value of <code>format.readName</code> such that
     * <code>fmt.toString().equals(fmt.readName)</code> is guaranteed to be
     * true.
     */
    @Override
    public String toString() {
        return name;
    }
}