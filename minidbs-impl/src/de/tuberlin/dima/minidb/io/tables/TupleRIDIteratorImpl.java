package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.util.Pair;

import java.util.Iterator;

public class TupleRIDIteratorImpl implements TupleRIDIterator{

    private int numCols;

    private long columnBitmap;

    private TablePage page;

    private int nextRecord;

    public TupleRIDIteratorImpl(TablePageImpl page){
        this.numCols = page.getSchema().getNumberOfColumns();

        columnBitmap = 0L;
        for (int i = 0; i < numCols; i++) {
            columnBitmap += 1L << i;
        }

        //this.columnBitmap = Long.MAX_VALUE;
        this.page = page;

        nextRecord = -1;
    }

    @Override
    public boolean hasNext() throws PageTupleAccessException {
        for (int i = nextRecord+1; i < page.getNumRecordsOnPage(); i++) {

            if (page.getDataTuple(i, columnBitmap, numCols) != null){
                return true;
            }

        }
        return false;
    }

    @Override
    public Pair<DataTuple, RID> next() throws PageTupleAccessException {
        if (!hasNext()) throw new PageTupleAccessException(page.getNumRecordsOnPage() + 1);

        for (int i = nextRecord+1; i <= page.getNumRecordsOnPage(); i++) {
            if (page.getDataTuple(i, columnBitmap, numCols) != null){
                nextRecord = i;
                break;
            }
        }

        DataTuple dataTuple = page.getDataTuple(nextRecord, columnBitmap, numCols);
        RID rid = new RID(((long)page.getPageNumber() << 32) + (long)nextRecord);

        return new Pair<>(dataTuple, rid);
    }
}
