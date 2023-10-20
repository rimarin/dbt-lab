package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

public class IndexLookupOperatorImpl implements IndexLookupOperator{
    private final DataField equalityLiteral;
    private final BTreeIndex index;
    private final DataField lowerBound;
    private final boolean lowerIncluded;
    private final DataField upperBound;
    private final boolean upperIncluded;

    private IndexResultIterator<RID> indexResultIterator;

    //wrong one I think
    //private final IndexScanOperator indexScanOperator;



    /**
     * Creates an index lookup operator that returns the RIDs for the key
     * given as the equality literal. This index scan is used to evaluate a local equality
     * predicate like <code> t1.someColumn = "Value" </code>.
     *
     * @param index The index object used to access the index.
     * @param equalityLiteral The key that the index returns the RIDs for.
     * @return An implementation of the IndexScanOperator.
     */
    public IndexLookupOperatorImpl(BTreeIndex index, DataField equalityLiteral) {
        this.index = index;
        this.equalityLiteral = equalityLiteral;
        this.lowerBound = null;
        this.lowerIncluded = false;
        this.upperBound = null;
        this.upperIncluded = false;
    }

    /**
     * Creates an index lookup operator returning the RIDs for the tuples in the given range.
     * This index scan is normally used to evaluate a between predicate:
     * <code> "val1" &lt;(=) key &lt;(=) "val2" </code>.
     * Note that any range predicate with only one bound can be converted to such a "between predicate" by
     * choosing the other bound as the minimal or maximal value in the domain.
     *
     * @param index The index object used to access the index.
     * @param lowerBound The lower bound of the range.
     * @param lowerIncluded Flag indicating whether the lower bound itself is included in the range.
     * @param upperBound The upper bound of the range.
     * @param upperIncluded Flag indicating whether the upper bound itself is included in the range.
     * @return An implementation of the IndexScanOperator.
     */
    public IndexLookupOperatorImpl(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound, boolean upperIncluded){
        this.index = index;
        this.equalityLiteral = null;
        this.lowerBound = lowerBound;
        this.lowerIncluded = lowerIncluded;
        this.upperBound = upperBound;
        this.upperIncluded = upperIncluded;

        //indexScanOperator = AbstractExtensionFactory.getExtensionFactory().createIndexScanOperator(index, lowerBound, upperBound, lowerIncluded, upperIncluded);
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
            if (equalityLiteral != null){ //was opened with first constructor
                indexResultIterator = index.lookupRids(equalityLiteral);
            } else { // was opened with second constructor
                indexResultIterator = index.lookupRids(lowerBound, upperBound, lowerIncluded, upperIncluded);
            }
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
    }

    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    @Override
    public void close() throws QueryExecutionException {
        indexResultIterator = null;
        System.out.println("IndexLookupOperatorImpl closed");
    }
}
