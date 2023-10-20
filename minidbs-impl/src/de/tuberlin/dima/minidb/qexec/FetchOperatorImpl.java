package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

import java.io.IOException;
import java.util.Arrays;

public class FetchOperatorImpl implements FetchOperator{
    private final PhysicalPlanOperator child;
    private final BufferPoolManager bufferPool;
    private final int tableResourceId;
    private final int[] outputColumnMap;
    private DataTuple correlatedTuple;


    private long columnBitmap;

    /**
     * Creates a new FETCH operator that takes RIDs to get tuples from a table.
     *
     * The FETCH operator implicitly performs a projection (no duplicate elimination)
     * by copying only some fields from the fetched tuple to the tuple that is produced.
     * This is described in the column-map-array. Position <tt>i</tt> in
     * that map array holds the position of the column in the fetched tuple that
     * goes to position <tt>i</tt> of the output tuple.
     * If the array contains {4, 1, 6} then the produced tuple contains only three columns,
     * namely the ones that are in the tables original columns 4, 1 and 6.
     *
     * @param child The child operator of this fetch operator.
     * @param bufferPool The buffer pool used to take the pages from.
     * @param tableResourceId The resource id of the table that the tuples are fetched from.
     * @param outputColumnMap The map describing how the column of the tuple produced by the
     *                        FETCH operator are produced from the tuple fetched from the table.
     * @return An implementation of the FetchOperator.
     */
    public FetchOperatorImpl(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {
        this.child = child;
        this.bufferPool = bufferPool;
        this.tableResourceId = tableResourceId;
        this.outputColumnMap = outputColumnMap;

        columnBitmap = Utils.getColumnBitmap(outputColumnMap);
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
        this.correlatedTuple = correlatedTuple;
        child.open(correlatedTuple);
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
        // this.bufferPool.
        DataTuple dataTuple;
        while ((dataTuple = child.next()) != null){
            RID rid = (RID) dataTuple.getField(0);
            try {
                CacheableData cacheableData = bufferPool.getPageAndPin(tableResourceId, rid.getPageIndex() );
                TablePage tablePage = (TablePage) cacheableData;
                DataTuple temp = tablePage.getDataTuple(rid.getTupleIndex(), columnBitmap, outputColumnMap.length);

                return Utils.reorderFields(temp, outputColumnMap);

            } catch (BufferPoolException | IOException e) {
                throw new QueryExecutionException(e);
            } catch (PageTupleAccessException e) {
                throw new RuntimeException(e);
            }
            //return dataTuple;
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
        child.close();
        //System.out.println("Fetch operator closed");
    }
}
