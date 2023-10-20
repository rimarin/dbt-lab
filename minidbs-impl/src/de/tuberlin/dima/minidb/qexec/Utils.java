package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;

import java.util.ArrayList;
import java.util.Arrays;

public class Utils {

    public static long getColumnBitmap(int[] columnIndexes){
        long columnBitmap = 0L;
        int[] columnIndices = Arrays.stream(columnIndexes).distinct().toArray();
        for (int i : columnIndices){
            columnBitmap += Math.pow(2, i);
        }
        return columnBitmap;
    }

    public static DataTuple reorderFields(DataTuple unordered, int[] columnIndexes){
        //random stuff I tried, without having time to finish with anything useful

        //DataTuple res = new DataTuple(producedColumnIndexes.length);

        ArrayList<Integer> sortedColumns = new ArrayList<>();
        for (int i = 0; i < columnIndexes.length; i++) {
            sortedColumns.add(columnIndexes[i]);
        }

        sortedColumns.sort(Integer::compare);

        ArrayList<Integer> indexSortedList = new ArrayList<>();
        int helper = 0;
        int lastColumn = -1;
        for (int i = 0; i < sortedColumns.size(); i++) {
            if (sortedColumns.get(i) == lastColumn) indexSortedList.add(helper);
            else {
                indexSortedList.add(helper);
                lastColumn = sortedColumns.get(i);
                helper++;
            }
        }

        DataTuple res = new DataTuple(columnIndexes.length);

        int tempHelper = 0;
        for (Integer column : columnIndexes){
            int indexInSortedList = sortedColumns.indexOf(column);
            int fieldIndexInTuple = indexSortedList.get(indexInSortedList);

            DataField field = unordered.getField(fieldIndexInTuple);
            res.assignDataField(field, tempHelper);
            tempHelper++;
        }

        return res;
    }
}
