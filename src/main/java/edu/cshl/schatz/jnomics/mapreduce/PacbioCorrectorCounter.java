package edu.cshl.schatz.jnomics.mapreduce;

import javax.smartcardio.ATR;

/**
 * User: james
 */
public class PacbioCorrectorCounter {

    public enum CoverageStatistics{
        BASES_AT_LEAST_ONE_COVERAGE,
        BASES_AT_LEAST_FIVE_COVERAGE,
        BASES_AT_LEAST_TWENTY_COVERAGE,
        BASES_AT_LEAST_HUNDRED_COVERAGE,
        TOTAL_BASES
    }
}
