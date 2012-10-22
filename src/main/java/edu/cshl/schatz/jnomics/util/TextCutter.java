/*
 * Copyright 2011 Matthew A. Titmus
 * 
 * This file is part of Jnomics.
 * 
 * Jnomics is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Jnomics is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * Jnomics. If not, see <http://www.gnu.org/licenses/>.
 */

package edu.cshl.schatz.jnomics.util;

import org.apache.hadoop.io.Text;

/**
 * A tool that provides a fast and easy means of extracting individual cuts from
 * delimited text.
 * 
 * @author Matthew A. Titmus
 */
/**
 * @author Matthew A. Titmus
 */
public class TextCutter {
    private static final byte TAB = '\t';

    /**
     * The number of cuts found, accessible via getCutCount().
     */
    private int cutCount = 0;

    /**
     * Cut properties lookup table: cutIndices[index] = { start index in input,
     * length in bytes }. Only indices < cutCount are valid.
     */
    private int[][] cutIndices = new int[10][2];

    /**
     * Input text gets copied into this Text object.
     */
    private Text defaultOutputText = new Text();

    private char delimiterChar = TAB;

    private boolean modFlag = false;

    /**
     * Input text gets copied into this Text object.
     */
    private Text sourceText = new Text();

    /**
     * Returns a {@link Text} instance containing the value of the requested
     * cut. Note that this method reuses an internal {@link Text} instance, and
     * that keeping a reference to it may yield unexpected results.
     * <p>
     * Negative cut indices are interpreted as
     * <code>{@link #getCutCount()} + cutIndex</code>. For example, a cutIndex
     * of -1 would return the last cut.
     * 
     * @param cutIndex The 0-based index of the desired cut. A negative value is
     *            interpreted as <code>{@link #getCutCount()} + cutIndex</code>.
     * @return A new Text() instance.
     * @throws ArrayIndexOutOfBoundsException if cutIndex is greater than or
     *             equal to the number of cuts.
     */
    public Text getCut(int cutIndex) {
        return getCut(defaultOutputText, cutIndex);
    }

    /**
     * Sets the contents of <code>text</code> to the value of the requested cut.
     * 
     * @param text The {@link Text} instance to reset.
     * @param cutIndex The 0-based index of the desired cut.
     * @return The passed {@link Text} instance text (not a copy).
     * @throws ArrayIndexOutOfBoundsException if cutIndex is greater than or
     *             equal to the number of cuts.
     */
    public Text getCut(Text text, int cutIndex) {
        if (modFlag) {
            reinitialize();
        }

        if (cutIndex < 0) {
            cutIndex = cutCount + cutIndex;
        }

        if (cutIndex >= cutCount) {
            throw new ArrayIndexOutOfBoundsException("Cut " + cutIndex
                    + " does not exist (cutCount=" + cutCount + ")");
        }

        int position = cutIndices[cutIndex][0];
        int length = cutIndices[cutIndex][1];

        text.set(sourceText.getBytes(), position, length);

        return text;
    }

    /**
     * Returns the number of cuts found by delimitting the input text by
     * {@link #getDelimiter()}.
     */
    public int getCutCount() {
        if (modFlag) {
            reinitialize();
        }

        return cutCount;
    }

    /**
     * Sets the contents of <code>text</code> to the value of the requested cuts
     * (including any intermediate delimiters). Note that this method reuses an
     * internal {@link Text} instance, and that keeping a reference to it may
     * yield unexpected results.
     * <p>
     * Negative cut indices are interpreted as
     * <code>{@link #getCutCount()} + cutIndex</code>. For example, a
     * <code>cutIndex</code> of -1 would return the last cut.
     * 
     * @param firstIndex The 0-based index of the first desired cut in the range
     *            (inclusive).
     * @param lastIndex The 0-based index of the last desired cut in the range
     *            (inclusive).
     * @return A new Text() instance.
     * @throws ArrayIndexOutOfBoundsException if <code>cutIndex</code> is
     *             greater than or equal to the number of cuts.
     */
    public Text getCutRange(int firstIndex, int lastIndex) {
        return getCutRange(defaultOutputText, firstIndex, lastIndex);
    }

    /**
     * Sets the contents of <code>text</code> to the value of the requested cuts
     * (including any intermediate delimiters).
     * <p>
     * Negative cut indices are interpreted as
     * <code>{@link #getCutCount()} + cutIndex</code>. For example, a
     * <code>cutIndex</code> of -1 would return the last cut.
     * <p>
     * If <code>lastIndex < firstIndex</code> (after converting negative indices
     * to their positive equivalents), then the resulting cut order is reversed.
     * For example, given the input "0 1 2 3 4", <code>getCutRange(4,2)</code>
     * and <code>getCutRange(-1,-3)</code> would both return "4 3 2".
     * 
     * @param text The {@link Text} instance to reset.
     * @param firstIndex The 0-based index of the first desired cut in the range
     *            (inclusive).
     * @param lastIndex The 0-based index of the last desired cut in the range
     *            (inclusive).
     * @return The passed {@link Text} instance text (not a copy).
     * @throws ArrayIndexOutOfBoundsException if cutIndex is greater than or
     *             equal to the number of cuts.
     */
    public Text getCutRange(Text text, int firstIndex, int lastIndex) {
        if (modFlag) {
            reinitialize();
        }

        if (firstIndex < 0) {
            firstIndex = cutCount + firstIndex;
        }

        if (lastIndex < 0) {
            lastIndex = cutCount + lastIndex;
        }

        if (lastIndex >= cutCount) {
            throw new ArrayIndexOutOfBoundsException("Requested cut does not exist (cutCount="
                    + cutCount + ")");
        }

        int position, length;

        if (firstIndex <= lastIndex) {
            position = cutIndices[firstIndex][0];
            length = lastIndex - firstIndex;

            for (int i = firstIndex; i <= lastIndex; i++) {
                length += cutIndices[i][1];
            }

            text.set(sourceText.getBytes(), position, length);
        } else {
            final byte[] delimBytes = new byte[] { (byte) delimiterChar };

            position = cutIndices[firstIndex][0];
            length = cutIndices[firstIndex][1];
            text.set(sourceText.getBytes(), position, length);

            for (int i = firstIndex - 1; i >= lastIndex; i--) {
                position = cutIndices[i][0];
                length = cutIndices[i][1];

                text.append(delimBytes, 0, 1);
                text.append(sourceText.getBytes(), position, length);
            }
        }

        return text;
    }

    /**
     * Get the character current being used as the cut delimiter. Default: tab (
     * <code>\t</code>).
     */
    public char getDelimiter() {
        return delimiterChar;
    }

    /**
     * Defines the text that the user wishes to get cuts of. This method copies
     * the entire string into the {@link TextCutter}.
     * 
     * @param text The text to cut get cuts of.
     * @return This instance.
     */
    public TextCutter set(String text) {
        sourceText.set(text);
        modFlag = true;
        return this;
    }

    /**
     * Defines the text that the user wishes to get cuts of. This method copies
     * characters <code>start</code> to <code>start + length - 1</code> from
     * <code>text</code> into the {@link TextCutter}.
     * 
     * @param text The source text.
     * @param start The 0-based position within the text to begin the cut.
     * @param length The number of characters to copy to the text cutter.
     * @return This instance.
     */
    public TextCutter set(String text, int start, int length) {
        sourceText.set(text);
        set(sourceText, start, length);
        modFlag = true;
        return this;
    }

    /**
     * Defines the text that the user wishes to get cuts of. The original
     * {@link Text} instance is copied, and isn't used directly.
     * 
     * @param text The text to cut get cuts of.
     * @return This instance.
     */
    public TextCutter set(Text text) {
        sourceText.set(text);
        modFlag = true;
        return this;
    }

    /**
     * Defines the text that the user wishes to get cuts of. The original
     * {@link Text} instance is copied, and isn't used directly.
     * 
     * @param text The text to cut get cuts of.
     * @param start The 0-based position within the text to begin the cut.
     * @param length The number of characters to copy to the text cutter.
     * @return This instance.
     */
    public TextCutter set(Text text, int start, int length) {
        sourceText.set(text.getBytes(), start, length);
        modFlag = true;
        return this;
    }

    /**
     * Redefines the character to use as the cut delimiter. Default: tab (
     * <code>\t</code>).
     * 
     * @return Itself.
     */
    public TextCutter setDelimiter(char delimiter) {
        delimiterChar = delimiter;
        modFlag = true;
        return this;
    }

    private void ensureColumnArraySize(int minsize) {
        if (cutIndices.length < minsize) {
            synchronized (cutIndices) {
                int[][] tmpColumns = new int[1 + (int) (1.2 * minsize)][2];

                System.arraycopy(cutIndices, 0, tmpColumns, 0, cutIndices.length);

                cutIndices = tmpColumns;
            }
        }
    }

    /**
     * Scans the internal {@link Text} instance and notes the cut positions and
     * sizes. Each cut is stored in <code>delimIndices</code> as
     * <code>int[2]{index, length}</code>.
     */
    private void reinitialize() {
        byte[] textBytes = sourceText.getBytes();
        int textLength = sourceText.getLength();
        int cut = 0, i;

        cutIndices[0][0] = 0;

        for (i = 0; i < textLength; i++) {
            if (textBytes[i] == delimiterChar) {
                ensureColumnArraySize(cut + 2);

                cutIndices[cut][1] = i - cutIndices[cut][0];
                cutIndices[++cut][0] = i + 1;
            }
        }

        cutIndices[cut][1] = i - cutIndices[cut][0];

        cutCount = cut + 1;
        modFlag = false;
    }
}