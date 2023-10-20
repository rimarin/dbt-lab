package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

public class TupleIteratorImpl implements TupleIterator{


    private int numCols;

    private long columnBitmap;

    private LowLevelPredicate[] preds;

    private TablePage page;

    private int nextRecord;

    public TupleIteratorImpl(LowLevelPredicate[] preds, int numCols, long columnBitmap, TablePage page){
        this.preds = preds;
        this.numCols = numCols;
        this.columnBitmap = columnBitmap;
        this.page = page;

        nextRecord = -1;
    }


    @Override
    public boolean hasNext() throws PageTupleAccessException {
        for (int i = nextRecord+1; i < page.getNumRecordsOnPage(); i++) {

            if (page.getDataTuple(preds, i, columnBitmap, numCols) != null){
                return true;
            }
        }
        return false;
    }

    @Override
    public DataTuple next() throws PageTupleAccessException {

        if (!hasNext()) throw new PageTupleAccessException(page.getNumRecordsOnPage() + 1);

        for (int i = nextRecord+1; i < page.getNumRecordsOnPage(); i++) {
            if (page.getDataTuple(preds, i, columnBitmap, numCols) != null){
                nextRecord = i;
                break;
            }
        }

        return page.getDataTuple(preds, nextRecord, columnBitmap, numCols);
    }
}
