package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

public class IndexCorrelatedLookupOperatorImpl implements IndexCorrelatedLookupOperator{
    private final BTreeIndex index;
    private final int correlatedColumnIndex;

    private IndexLookupOperator indexLookupOperator;

    private IndexResultIterator<RID> indexResultIterator;
    /**
     * Creates an index lookup operator that works in a correlated fashion. For each time it is opened,
     * it returns the RIDs for the key equal to the correlated tuple's column at the specified position.
     *
     * @param index The index object used to access the index.
     * @param correlatedColumnIndex The index of the column in the correlated tuple that we evaluate against.
     * @return An implementation of the IndexCorrelatedScanOperator.
     */
    public IndexCorrelatedLookupOperatorImpl(BTreeIndex index, int correlatedColumnIndex) {
        this.index = index;
        this.correlatedColumnIndex = correlatedColumnIndex;
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
        DataField dataField = index.getIndexSchema().getIndexTableSchema().getColumn(correlatedColumnIndex).getDataType().getNullValue();
        if (correlatedTuple == null){
            System.out.println("WHYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY");
            indexLookupOperator = new IndexLookupOperatorImpl(index, dataField);
        } else {
            try {
                indexResultIterator = index.lookupRids(correlatedTuple.getField(correlatedColumnIndex));
            } catch (PageFormatException | IOException e) {
                throw new RuntimeException(e);
            }
            //indexLookupOperator = new IndexLookupOperatorImpl(index, correlatedTuple.getField(correlatedColumnIndex));
        }

        /*try {
            IndexResultIterator<RID> indexResultIterator = index.lookupRids(correlatedTuple.getField(correlatedColumnIndex));
        } catch (PageFormatException | IOException e) {
            throw new RuntimeException(e);
        }*/

        //indexLookupOperator.open(correlatedTuple);
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
        /*if ( indexLookupOperator == null) {
            System.out.println("weird");
            return null;
        }
        return indexLookupOperator.next();*/
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
    }

    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    @Override
    public void close() throws QueryExecutionException {
        //indexLookupOperator.close();
    }
}
