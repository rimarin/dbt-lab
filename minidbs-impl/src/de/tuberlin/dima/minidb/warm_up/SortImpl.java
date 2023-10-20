package de.tuberlin.dima.minidb.warm_up;

import java.util.ArrayList;
import java.util.Comparator;

public class SortImpl implements Sort{

    /**
     * This method sorts the input table according to the comparison rule of the Comparable type
     *
     * @param table
     */
    @Override
    @SuppressWarnings("rawtypes")
    public ArrayList<Comparable> sort(ArrayList<Comparable> table) {
        Comparator<Comparable> c = (o1, o2) -> o1.compareTo(o2);
        table.sort(c);
        return table;
    }
}
