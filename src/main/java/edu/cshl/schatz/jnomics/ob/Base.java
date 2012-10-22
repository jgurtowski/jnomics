/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * An enumeration containing all standard IUPAC nucleotide codes, as defined
 * described in <i><a
 * href="http://www.chem.qmul.ac.uk/iubmb/misc/naseq.html">Nomenclature for
 * Incompletely Specified Bases in Nucleic Acid Sequences</a></i> (Nomenclature
 * Committee of the International Union of Biochemistry (NC-IUB), 1985).
 * 
 * <pre>
 * A   Adenosine
 * C   Cytosine
 * G   Guanine
 * T   Thymidine 
 * U   Uracil
 * R   G A (puRine)
 * Y   T C (pYrimidine)
 * K   G T (Ketone)
 * M   A C (aMino group)
 * S   G C (Strong interaction)
 * W   A T (Weak interaction)
 * B   G T C (not A) (B comes after A)
 * D   G A T (not C) (D comes after C)
 * H   A C T (not G) (H comes after G)
 * V   G C A (not T, not U) (V comes after U)
 * N   A G C T (aNy)
 * X   masked
 * -   gap of indeterminate length
 * </pre>
 * 
 * Its primary purpose is to provide an easy mechanism for parsing and providing
 * IUPAC nucleotide codes, determining complements and relative "GC-ness" and
 * "AT-ness"; currently it isn't used in any data structures to represent a base
 * in a sequence.
 * 
 * @see <a href="http://en.wikipedia.org/wiki/Nucleic_acid_notation">Wikipedia,
 *      Nucleic acid notation</a>
 * @see <a href="http://www.chem.qmul.ac.uk/iubmb/misc/naseq.html">Nomenclature
 *      for Incompletely Specified Bases in Nucleic Acid Sequences,
 *      <i>Nomenclature Committee of the International Union of Biochemistry
 *      (NC-IUB)</i>, 1985</a>
 */
public enum Base {
    A('A', 'T', Codes.A),
    B('B', 'V', Codes.C | Codes.T | Codes.G),
    C('C', 'G', Codes.C),
    D('D', 'H', Codes.A | Codes.T | Codes.G),
    G('G', 'C', Codes.G),
    H('H', 'D', Codes.A | Codes.C | Codes.T),
    HYPHEN('-', '-', Codes.IGNORE),
    K('K', 'M', Codes.T | Codes.G),
    M('M', 'K', Codes.A | Codes.C),
    N('N', 'N', Codes.A | Codes.C | Codes.T | Codes.G),
    R('R', 'Y', Codes.A | Codes.G),
    S('S', 'W', Codes.G | Codes.C),
    T('T', 'A', Codes.T),
    U('U', 'A', Codes.T),
    V('V', 'V', Codes.A | Codes.C | Codes.G),
    W('W', 'S', Codes.A | Codes.T),
    X('X', 'X', Codes.A | Codes.C | Codes.T | Codes.G),
    Y('Y', 'R', Codes.C | Codes.T);

    /**
     * This allows any legal base code to be quickly converted into its byte
     * representation (the code's upper-case ASCII value). Each base's
     * respective {@link Base} enum value is contained at position
     * <code>ch & Byte.MAX_VALUE</code>; all other positions contain
     * <code>null</code>.
     */
    private final static Base[] CHAR_TO_BASE_LOOKUP = new Base[Byte.MAX_VALUE];

    /**
     * The byte value used to represent this base. It is simply the byte value
     * of the base's upper-case IUPAC character code.
     */
    public final byte code;

    /**
     * The "G/C" weight. Equal to <tt>1.0</tt> for {@link #G}, {@link #C}, and
     * {@link #S} and <tt>0.0</tt> for {@link #A}, {@link #T}, and {@link #W};
     * degenerate bases that represent some combination of G/C and A/T will have
     * intermediate values. For any given base {@link #gcRatioWeight} and
     * {@link #atRatioWeight} generally sum to <tt>1.0</tt> (except for
     * {@link #HYPHEN}, which is always <tt>0.0</tt>).
     */
    public final float gcRatioWeight;

    /**
     * The "A/T" weight. Equal to <tt>1.0</tt> for {@link #A}, {@link #T}, and
     * {@link #W} and <tt>0.0</tt> for {@link #G}, {@link #C}, and {@link #S};
     * degenerate bases that represent some combination of G/C and A/T will have
     * intermediate values. For any given base {@link #gcRatioWeight} and
     * {@link #atRatioWeight} generally sum to <tt>1.0</tt> (except for
     * {@link #HYPHEN}, which is always <tt>0.0</tt>).
     */
    public final float atRatioWeight;

    /**
     * The relative probabilities that a base represented by the code is an
     * {@link #A}. {@link #pA} + {@link #pC} +{@link #pT} +{@link #pG} =
     * <tt>1.0</tt> for all base codes (except '<tt>-</tt>', for which the
     * probability is always <tt>0.0</tt>).
     */
    public final float pA;

    /**
     * The relative probabilities that a base represented by the code is a
     * {@link #C}. {@link #pA} + {@link #pC} +{@link #pT} +{@link #pG} =
     * <tt>1.0</tt> for all base codes (except '<tt>-</tt>', for which the
     * probability is always <tt>0.0</tt>).
     */
    public final float pC;

    /**
     * The relative probabilities that a base represented by the code is a
     * {@link #T}. {@link #pA} + {@link #pC} +{@link #pT} +{@link #pG} =
     * <tt>1.0</tt> for all base codes (except '<tt>-</tt>', for which the
     * probability is always <tt>0.0</tt>).
     */
    public final float pT;

    /**
     * The relative probabilities that a base represented by the code is a
     * {@link #G}. {@link #pA} + {@link #pC} +{@link #pT} +{@link #pG} =
     * <tt>1.0</tt> for all base codes (except '<tt>-</tt>', for which the
     * probability is always <tt>0.0</tt>).
     */
    public final float pG;

    /**
     * The byte code of this base's reverse complement.
     * 
     * @see #code
     */
    public final byte reverseComplementCode;

    private Base reverseComplement = null;

    static {
        for (Base b : Base.values()) {
            CHAR_TO_BASE_LOOKUP[Character.toUpperCase(b.code) & Byte.MAX_VALUE] = b;
            CHAR_TO_BASE_LOOKUP[Character.toLowerCase(b.code) & Byte.MAX_VALUE] = b;
        }
    }

    private Base(char code, char complement, int identities) {
        float a = 0f, c = 0f, t = 0f, g = 0f, gc = 0f, at = 0f, matches = 0f;

        this.code = (byte) Character.toUpperCase(code);
        reverseComplementCode = (byte) Character.toUpperCase(complement);

        if (identities > 0) {
            if ((identities & Codes.G) == Codes.G) {
                matches += g = 1f;
            }
            if ((identities & Codes.C) == Codes.C) {
                matches += c = 1f;
            }
            if ((identities & Codes.A) == Codes.A) {
                matches += a = 1f;
            }
            if ((identities & Codes.T) == Codes.T) {
                matches += t = 1f;
            }

            gc = (g + c) / (g + c + a + t);
            at = (a + t) / (g + c + a + t);

            a = a / matches;
            c = c / matches;
            g = g / matches;
            t = t / matches;
        }

        pA = a;
        pC = c;
        pG = g;
        pT = t;
        gcRatioWeight = gc;
        atRatioWeight = at;
    }

    /**
     * Provides a simple means to retrieve {@link Base} value, given a that
     * bases character code as a char, byte, or integer (case-insensitive).
     * 
     * @return The desired base, or <code>null</code> if <code>base</code> isn't
     *         a valid IUPAC code.
     */
    public static final Base characterToBase(int base) {
        return CHAR_TO_BASE_LOOKUP[base & Byte.MAX_VALUE];
    }

    /**
     * Returns the {@link Base} representation of this base's reverse
     * complement.
     */
    public Base getComplement() {
        if (reverseComplement == null) {
            reverseComplement = characterToBase(reverseComplementCode);
        }

        return reverseComplement;
    }

    private static class Codes {
        static final int A = 1, C = 2, T = 4, G = 8, IGNORE = 0;
    }

    @SuppressWarnings({ "unused", "synthetic-access" })
    private static class Tests {
        static void charToBaseLookup() {
            char[] eval = new char[] {
                    'A',
                    'C',
                    'T',
                    'G',
                    'R',
                    'Y',
                    'K',
                    'M',
                    'X',
                    '-',
                    'n',
                    'b',
                    'z' };

            for (char ch : eval) {
                int i;

                ch = Character.toUpperCase(ch);
                i = (byte) (ch & Byte.MAX_VALUE);
                System.out.println(ch + " = " + CHAR_TO_BASE_LOOKUP[i].code);

                ch = Character.toLowerCase(ch);
                i = (byte) (ch & Byte.MAX_VALUE);
                System.out.println(ch + " = " + CHAR_TO_BASE_LOOKUP[i].code);
            }
        }

        static void charToGCLookup() {
            char[] eval = new char[] {
                    'G',
                    'C',
                    'A',
                    'T',
                    'U',
                    'R',
                    'Y',
                    'K',
                    'M',
                    'S',
                    'W',
                    'B',
                    'D',
                    'H',
                    'V',
                    'N',
                    'K',
                    'X',
                    '-' };

            for (char ch : eval) {
                Base bc = CHAR_TO_BASE_LOOKUP[ch];

                System.out.println(ch + ": GC=" + bc.gcRatioWeight);
            }
        }
    }
}
