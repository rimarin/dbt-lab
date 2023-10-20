package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;


import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

public class TableScanOperatorImpl implements TableScanOperator{

    private final BufferPoolManager bufferPool;

    private final TableResourceManager tableManager;

    private final int resourceId;

    private final int[] producedColumnIndexes;

    private final LowLevelPredicate[] predicate;

    private final int prefetchWindowLength;

    private final int prefetchThreshold; // would be advanced. Always prefetch when not enough pages are fetched. Right now just fetch if no pages are fetched

    private TupleIterator tupleIterator;

    private final ArrayList<DataTuple> fetchedTuples;

    private int currentPage;

    private long columnBitmap;

    private final int distinctAmountColumns;

    public TableScanOperatorImpl(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId, int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {
        this.bufferPool = bufferPool;
        this.tableManager = tableManager;
        this.resourceId = resourceId;
        this.producedColumnIndexes = producedColumnIndexes;
        this.predicate = predicate;
        this.prefetchWindowLength = prefetchWindowLength;
        this.prefetchThreshold = prefetchWindowLength / 3;
        this.fetchedTuples = new ArrayList<>();
        this.currentPage = tableManager.getFirstDataPageNumber();

        columnBitmap = 0L;
        int[] columnIndices = Arrays.stream(producedColumnIndexes).distinct().toArray();
        for (int i : columnIndices){
            columnBitmap += Math.pow(2, i);
        }
        distinctAmountColumns = columnIndices.length;
    }

    /**
     * Opens the plan below and including this operator. Sets the initial status
     * necessary to start executing the plan.
     * <p>
     * In the case that the tuples produced by the plan rooted at this operator
     * are correlated to another stream of tuples, the plan is opened and closed for
     * every such. The OPEN call gets as parameter the tuple for which the correlated
     * tuples are to be produced this time.
     * The most common example for that is the inner side of a nested-loop-join,
     * which produces tuples correlated to the current tuple from the outer side
     * (outer loop).
     * <p>
     * Consider the following pseudocode, describing the next() function of a
     * nested loop join:
     * <p>
     * // S is the outer child of the Nested-Loop-Join.
     * // R is the inner child of the Nested-Loop-Join.
     * <p>
     * next() {
     * do {
     * r := R.next();
     * if (r == null) { // not found, R is exhausted for the current s
     * R.close();
     * s := S.next();
     * if (s == null) { // not found, both R and S are exhausted
     * return null;
     * }
     * R.open(s);    // open the plan correlated to the outer tuple
     * r := R.next();
     * }
     * } while ( r.keyColumn != s.keyColumn ) // until predicate is fulfilled
     * <p>
     * return concatenation of r and s;
     * }
     *
     * @param correlatedTuple The tuple for which the correlated tuples in the sub-plan
     *                        below and including this operator are to be fetched. Is
     *                        null is the case that the plan produces no correlated tuples.
     * @throws QueryExecutionException Thrown, if the operator could not be opened,
     *                                 that means that the necessary actions failed.
     */
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        if (currentPage > tableManager.getLastDataPageNumber()) return;

        CacheableData tablePageTemp;
        try {
            //bufferPool.registerResource(resourceId, tableManager);
            //bufferPool.startIOThreads();
            tablePageTemp = bufferPool.getPageAndPin(resourceId, currentPage);
        } catch (BufferPoolException | IOException e) {
            throw new QueryExecutionException(e);
        }

        if (tablePageTemp instanceof TablePage){

            TablePage tablePage = (TablePage) tablePageTemp;

            try {
                tupleIterator = tablePage.getIterator(predicate, distinctAmountColumns, columnBitmap);
            } catch (PageTupleAccessException e) {
                throw new QueryExecutionException(e);
            }

            prefetchTuples();

        }
        else throw new QueryExecutionException("not a table");
    }

    /**
     * This gets the next tuple from the operator.
     * This method should return null, if no more tuples are available.
     *
     * @return The next tuple, produced by this operator.
     * @throws QueryExecutionException Thrown, if the query execution could not be completed
     *                                 for whatever reason.
     */
    @Override
    public DataTuple next() throws QueryExecutionException {
        if (tupleIterator == null) throw new QueryExecutionException();

        while (fetchedTuples.isEmpty()){
            if (currentPage > tableManager.getLastDataPageNumber()) return null;
            else prefetchTuples();
        }

        return fetchedTuples.remove(0);
    }


    private void prefetchTuples() throws QueryExecutionException {

        //fetchedTuples should be 0 at start
        for (int i = fetchedTuples.size(); i < prefetchWindowLength; i++) {
            try {
                if (tupleIterator.hasNext()){
                    DataTuple temp = tupleIterator.next();

                    fetchedTuples.add(sortDataTupleColumns(temp));
                } else {
                    currentPage++;
                    open(null);
                }
            } catch (PageTupleAccessException e) {
                throw new QueryExecutionException(e);
            }
        }
    }

    /**
     * this methods implementation is absolute trash right now.
     * What it is supposed to do is the following. The dataTuple we get from the iterator contains it fields in an unsorted way
     * and it does not contain any field twice, even if producedColumnIndexes contains the column twice. So we need to use the
     * dataTuple that we got to create a new dataTuple that has everything sorted.
     * @param dataTuple the dataTuple that contains each wanted column exactly once and that is unsorted
     * @return a new dataTuple that has the columns correctly sorted and that contains columns twice if we want it to.
     */
    private DataTuple sortDataTupleColumns(DataTuple dataTuple){

        //random stuff I tried, without having time to finish with anything useful

        //DataTuple res = new DataTuple(producedColumnIndexes.length);

        ArrayList<Integer> sortedColumns = new ArrayList<>();
        for (int i = 0; i < producedColumnIndexes.length; i++) {
            sortedColumns.add(producedColumnIndexes[i]);
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

        DataTuple res = new DataTuple(producedColumnIndexes.length);

        int tempHelper = 0;
        for (Integer column : producedColumnIndexes){
            int indexInSortedList = sortedColumns.indexOf(column);
            int fieldIndexInTuple = indexSortedList.get(indexInSortedList);

            DataField field = dataTuple.getField(fieldIndexInTuple);
            res.assignDataField(field, tempHelper);
            tempHelper++;
        }

        return res;

    }

    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    @Override
    public void close() throws QueryExecutionException {
        System.out.println("Dont know what to do yet");
        //bufferPool.closeBufferPool();
        /*try {
            tableManager.closeResource();
        } catch (IOException e) {
            throw new QueryExecutionException("closing of tableManager failed");
        }*/

    }
}
