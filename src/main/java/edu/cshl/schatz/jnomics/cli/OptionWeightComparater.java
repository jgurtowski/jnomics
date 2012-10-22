/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.cli;

import java.util.Comparator;

import org.apache.commons.cli.Option;

public class OptionWeightComparater implements Comparator<Option> {
    public int compare(Option o1, Option o2) {
        int wt1 = WeightedOption.DEFAULT_WEIGHT;
        int wt2 = WeightedOption.DEFAULT_WEIGHT;

        if (o1 instanceof WeightedOption) {
            wt1 = ((WeightedOption) o1).getWeight();
        }

        if (o2 instanceof WeightedOption) {
            wt2 = ((WeightedOption) o2).getWeight();
        }

        if (wt1 != wt2) {
            return wt2 - wt1;
        } else {
            String opt1 = o1.getOpt();
            String opt2 = o2.getOpt();

            return opt1.compareToIgnoreCase(opt2);
        }
    }
}