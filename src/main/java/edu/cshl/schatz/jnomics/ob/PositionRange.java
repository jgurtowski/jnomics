/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * Represents the first position and length of a subsequence with respect to a
 * reference sequence. The first and last indices (inclusive) are accessible via
 * the {@link #first()} and {@link #last()} methods, respectively.
 * 
 * @author Matthew Titmus
 */
public class PositionRange {
    /**
     * The default value for the index of the left-most base in the sequence,
     * typically referred to in this package as the sequence's "first position".
     * By convention, the first position is set to 1, though some
     * implementations may prefer a 0-based indexing system.
     */
    public static final int DEFAULT_FIRST_POSITION = 1;

    private int first, length;

    /**
     * Creates a new instance with a first position of DEFAULT_FIRST_POSITION
     * (1) and a length of 0.
     */
    protected PositionRange() {
        this(DEFAULT_FIRST_POSITION, 0);
    }

    /**
     * Creates a new instance given a first base position index and length. The
     * final endpoints will be <code>[first..(first + length - 1)]</code>.
     * 
     * @param first The new index of the first base in the sequence.
     * @param length The new length of the sequence.
     */
    protected PositionRange(int first, int length) {
        this.first = first;
        this.length = length;
    }

    /**
     * Creates a new {@link PositionRange} that is a copy of an existing
     * {@link PositionRange} instance.
     * 
     * @param toCopy The PositionRange to copy.
     * @return A new instance, such that
     *         <code>toCopy.equals(instanceByCopy(toCopy))</code> returns
     *         <code>true</code>.
     */
    public static PositionRange instanceByCopy(PositionRange toCopy) {
        return instanceByLength(toCopy.first(), toCopy.length());
    }

    /**
     * Creates a new {@link PositionRange} instance from the provided first and
     * last base position indices. The resulting endpoints will be
     * <code>[first..last]</code>, and the length will be
     * <code>last - first + 1</code>.
     * 
     * @param first The new index of the first base in the sequence.
     * @param last The new index of the last base in the sequence (inclusive).
     * @throws IndexOutOfBoundsException If the <code>first</code> >
     *             <code>last</code>
     */
    public static PositionRange instanceByEnds(int first, int last) {
        if (first > last) {
            throw new IndexOutOfBoundsException("first > last");
        }

        return new PositionRange(first, (last - first) + 1);
    }

    /**
     * Creates a new instance given a first base position index and length. The
     * final endpoints will be <code>[first..(first + length - 1)]</code>.
     * 
     * @param first The new index of the first base in the sequence.
     * @param length The new length of the sequence.
     * @return A new <code>PositionRange</code> instance.
     */
    public static PositionRange instanceByLength(int first, int length) {
        return new PositionRange(first, length);
    }

    /**
     * Returns <code>true</code> if <code>index</code> exists in the range of
     * <code>first <= index <= last</code>.
     */
    public boolean contains(int index) {
        return ((first <= index) && (last() >= index));
    }

    /**
     * Returns <code>true</code> if the range
     * <code>index..(index + length - 1)</code> exists entirely in the range of
     * <code>first <= i <= last</code>.
     * 
     * @param start The first index of the range to check.
     * @param length The new length of the range to check (including
     *            <code>index</code>).
     * @return <code>true</code> if the range falls entirely within the
     *         sequence; <code>false</code> otherwise.
     */
    public boolean contains(int start, int length) {
        return ((first <= start) && (last() >= ((start + length) - 1)));
    }

    /**
     * Returns <code>true</code> if the range described by <code>subrange</code>
     * exists entirely in the <code>first <= i <= last</code>.
     * 
     * @param subrange The range to check.
     * @return <code>true</code> if <code>range</code> falls entirely within the
     *         range represented by this object; <code>false</code> otherwise.
     */
    public boolean contains(PositionRange subrange) {
        return contains(subrange.first(), subrange.length());
    }

    /**
     * Returns <code>true</code> if <code>obj</code> is an instance of
     * {@link PositionRange}, and has values of {@link #first()} and
     * {@link #length()} that match this instance.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PositionRange) {
            PositionRange p = (PositionRange) obj;

            return (first() == p.first()) && (length() == p.length());
        } else {
            return false;
        }
    }

    /**
     * Returns the index of the first position represented by this sequence.
     */
    public int first() {
        return first;
    }

    /**
     * Returns a two-index integer array containing the array indices of the
     * first and last bases in the relevant sequence.
     * 
     * @return A two-index integer array; or <code>null</code> if the length
     *         attribute is 0.
     */
    public int[] getEndpoints() {
        if (length() > 0) {
            return new int[] { first, last() };
        } else {
            return null;
        }
    }

    /**
     * Generates a hashCode based on the first and last positions in this range.
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return (first() << 16) & last();
    }

    /**
     * Returns the index of the last position represented by this sequence,
     * equal to <code>{@link #first()} + {@link #last()}</code>.
     */
    public int last() {
        return (first + length()) - 1;
    }

    /**
     * Returns the total length of the range, equal to
     * <code>{@link #last()} - {@link #first()} + 1</code>.
     * 
     * @return The total length of the range.
     */
    public int length() {
        return this.length;
    }

    /**
     * Sets <code>range</code> to contain the values of the range overlapping
     * this range. If the ranges overlap the length is set to the number of
     * overlapping positions; otherwise the value is 0 or negative, reflecting
     * the distance between the ranges.
     * 
     * @param range The range instance to determine the overlaps of. <i>This
     *            object is modified by this method.</i>
     * @throws UnsupportedOperationException If this {@link PositionRange}
     *             implementation doesn't permit modification of its length.
     */
    public void overlap(PositionRange range) {
        int start2, end1, end2;

        if (first <= range.first) {
            start2 = range.first;
            end1 = last();
            end2 = range.last();
        } else {
            start2 = first;
            end1 = range.last();
            end2 = last();
        }

        range.setFirst(start2);
        range.setLength(((end1 >= end2 ? end2 : end1) - start2) + 1);
    }

    /**
     * Returns a {@link String} representation of this {@link PositionRange},
     * containing its first and last positions.
     * 
     * @return Returns a {@link String} representation of this
     *         {@link PositionRange}.
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "[" + first() + ".." + last() + "]";
    }

    /**
     * Sets this {@link PositionRange} instance to be identical to the one
     * passed in the toCopy<code>toCopy</code> parameter.
     * 
     * @param toCopy The <code>PositionRange</code> to copy.
     * @throws UnsupportedOperationException If this {@link PositionRange}
     *             implementation doesn't permit modification of its length.
     */
    protected void set(PositionRange toCopy) {
        setFirst(toCopy.first);
        setLength(toCopy.length());
    }

    /**
     * Allows this <code>PositionRange</code>'s endpoint positions to be
     * modified simultaneously. The resulting endpoints will be
     * <code>[first..last]</code>, and the length will be
     * <code>last - first + 1</code>.
     * 
     * @param first The new index of the first base in the sequence.
     * @param last The new index of the last base in the sequence (inclusive).
     * @throws IndexOutOfBoundsException If <code>first</code> >
     *             <code>last</code>
     * @throws UnsupportedOperationException If this {@link PositionRange}
     *             implementation doesn't permit modification of its length.
     */
    protected void setEndpoints(int first, int last) {
        setFirst(first);
        setLength((1 + last) - first);
    }

    /**
     * Sets the position of the leftmost base in the sequence. By convention,
     * the default value of the first position is <code>1</code>.
     * 
     * @param first The new index of the first base in the sequence.
     */
    protected void setFirst(int first) {
        this.first = first;
    }

    /**
     * Sets the length property of the represented range; the last base position
     * is also adjusted appropriately.
     * 
     * @param length The new length value.
     * @throws UnsupportedOperationException If this {@link PositionRange}
     *             implementation doesn't permit modification of its length.
     */
    protected void setLength(int length) {
        this.length = length;
    }
}
