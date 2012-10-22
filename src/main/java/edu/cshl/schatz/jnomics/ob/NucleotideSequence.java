/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * <p>
 * This class represents a sequence of nucleotides, and provides methods for
 * their manipulation and analysis.
 * </p>
 * 
 * @author Matthew Titmus
 */
public interface NucleotideSequence extends CharSequence {
    /**
     * Resets all fields to their default value.
     */
    public void clear();

    /**
     * The sum of the count of A/T bases in this sequence, such that the values
     * of <code>getGCSum() + getATSum()</code> equals the sequence length, minus
     * the number of masked bases and gaps.
     */
    public double getATSum();

    /**
     * Returns a copy of the bases array with a length of
     * <code>getLength()</code>.
     */
    public byte[] getBases();

    /**
     * If this sequence maps to a reference sequence (chr1, for example), this
     * contains the indices of the first and last bases the sequence maps or
     * aligns to. If it doesn't, the start position is 1.
     */
    public PositionRange getEndpoints();

    /**
     * Returns the GC content of this sequence. Non-GCAT codes are counted
     * according to the proportion of the various bases it may represent. For
     * example, B (G, C, or T) is counted as GC=0.75 and AT=0.25. Masked bases
     * ('X') and gaps ('-') are not counted.
     * 
     * @return A floating point value from 0.0 to 1.0, inclusive.
     */
    public double getGCContent();

    /**
     * The sum of the count of G/C bases in this sequence, such that the values
     * of <code>getGCSum() + getATSum()</code> equals the sequence length, minus
     * the number of masked bases and gaps.
     */
    public double getGCSum();

    /**
     * The orientation of this sequence relative to the reference. Default:
     * PLUS.
     */
    public Orientation getOrientation();

    /**
     * Returns the underlying byte array. It may be of any length >=
     * getLength(), but only the first <code>getLength()</code> bytes are
     * meaningful.
     */
    public byte[] getRawBytes();

    /**
     * Gets the name of the reference sequence, such as "chr21", or "NC_12345".
     * 
     * @return The reference name. An empty string indicates an unset value.
     */
    public String getReferenceName();

    /**
     * A convenience method that provides an easy means to convert the internal
     * byte to a String. This version will encode the entire internal array, so
     * use caution with very large sequences.
     */
    public String getSequenceString();

    /**
     * The number of sequence positions occupied by this sequence (the number of
     * bases plus the number of masked bases and gaps).
     */
    public int length();

    /**
     * Reverse complements this nucleotide sequence and inverts the orientation.
     * 
     * @throws IUPACCodeException if an invalid nucleotide code is found in the
     *             sequence (see http://www.bioinformatics.org/sms/iupac.html).
     */
    public void reverseComplement();

    /**
     * Sets this sequence to be an identical copy of the
     * <code>sourceSequence</code> parameter. The result is effectively a deep
     * clone (i.e., no references are shared between this instance and those
     * used by <code>sourceSequence</code>.
     */
    public void set(NucleotideSequence sequence);

    /**
     * Resets the orientation of this Nucleotide sequence.
     */
    public void setOrientation(Orientation orientation);

    /**
     * Directly sets the underlying sequence bytes to the contents of
     * <code>bases</code> from position <code>start</code> to
     * <code>start + length - 1</code>, inclusive.
     * <p>
     * This is designed to be a speedy, system-level operation and provides no
     * validation or safety checks, and should be used with caution. Whether the
     * array is used directly or copied into an internal buffer is not
     * specified.
     * 
     * @param bases The nucleotides sequence as an array of 8-bit IUPAC
     *            character codes (case-insensitive).
     * @param start The array index of the first base.
     * @param length The number of positions of <code>bases</code> to set to.
     * @throws ArrayStoreException if an element in the <code>src</code> array
     *             could not be stored into the <code>dest</code> array because
     *             of a type mismatch.
     * @throws IndexOutOfBoundsException if copying would cause access of data
     *             outside array bounds.
     * @throws NullPointerException if either <code>bases</code> or the internal
     *             buffer is <code>null</code>.
     */
    public void setRawBytes(byte[] bases, int start, int length);

    /**
     * Sets the name of the reference sequence, such as "chr21", or "NC_12345".
     * If not set, this will contain an empty string.
     */
    public void setReferenceName(String referenceName);

    /**
     * Returns a new <code>NucleotideSequence</code> that is a subsequence of
     * this sequence. The subsequence starts with the value at the specified
     * index and ends with the value at index <tt>end - 1</tt>. The length (in
     * <code>char</code>s) of the returned sequence is <tt>end - start</tt>, so
     * if <tt>start == end</tt> then an empty sequence is returned. </p>
     * 
     * @param start the start index, inclusive
     * @param end the end index, exclusive
     * @return the specified subsequence
     * @throws IndexOutOfBoundsException if <tt>start</tt> or <tt>end</tt> are
     *             negative, if <tt>end</tt> is greater than <tt>length()</tt>,
     *             or if <tt>start</tt> is greater than <tt>end</tt>
     * @see java.lang.CharSequence#subSequence(int, int)
     */
    public NucleotideSequence subSequence(int start, int end);
}
