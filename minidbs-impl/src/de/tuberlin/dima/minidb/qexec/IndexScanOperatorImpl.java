package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

public class IndexScanOperatorImpl implements IndexScanOperator{

    private final BTreeIndex index;

    private final DataField startKey;

    private final DataField stopKey;

    private final boolean startKeyIncluded;

    private final boolean stopKeyIncluded;

    private IndexResultIterator<DataField> indexResultIterator;

    public IndexScanOperatorImpl(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
        this.index = index;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.startKeyIncluded = startKeyIncluded;
        this.stopKeyIncluded = stopKeyIncluded;
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

        try {
            indexResultIterator = index.lookupKeys(startKey, stopKey, startKeyIncluded, stopKeyIncluded);
        } catch (PageFormatException | IOException e) {
            throw new QueryExecutionException(e);
        }
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
        try {
            if (indexResultIterator.hasNext()){
                DataField[] tempDataFieldArray = new DataField[1];
                tempDataFieldArray[0] = indexResultIterator.next();
                return new DataTuple(tempDataFieldArray);
            }
        } catch (PageFormatException | IOException e) {
            throw new QueryExecutionException(e);
        }

        return null;
        /*IndexSchema indexSchema = index.getIndexSchema();
        ColumnSchema columnSchema = indexSchema.getIndexedColumnSchema();
        TableSchema tableSchema = indexSchema.getIndexTableSchema();


        int start = index.getIndexSchema().getFirstLeafNumber();

        DataField dataField2 = null;
        while (start < 10){
            start++;
            try {
                if (iterator.hasNext()){
                     dataField2 = iterator.next();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (PageFormatException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            IndexResultIterator<RID> ridIterator = index.lookupRids(dataField2);

            if (ridIterator.hasNext()){
                RID test = ridIterator.next();
                test.getPageIndex();


            }


        } catch (PageFormatException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return null;*/
    }

    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    @Override
    public void close() throws QueryExecutionException {

    }
}
