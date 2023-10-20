package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class FilterCorrelatedOperatorImpl implements FilterCorrelatedOperator{
    private final JoinPredicate correlatedPredicate;
    private final PhysicalPlanOperator child;
    private DataTuple correlatedTuple;

    /**
     * Creates a new filter operator that evaluates a local predicate
     * and also a predicate against a correlated tuple. The tuple stream below the
     * filter will hence be uncorrelated, the tuple stream above the filter will be
     * correlated.
     *
     * @param child The child of this operator.
     * @param correlatedPredicate The correlated predicate. The predicate is constructed such that
     *                            the current tuple will be the left hand argument and the correlated tuple
     *                            the right hand argument.
     * @return An implementation of the FilterOperator.
     */
    public FilterCorrelatedOperatorImpl(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
        this.child = child;
        this.correlatedPredicate = correlatedPredicate;
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
        // Filter tuples from the child operator according to the correlated predicate and the correlated tuple
        DataTuple eval;
        while((eval = child.next()) != null) {
            // Return the filtered tuple
            if (correlatedPredicate.evaluate(eval, correlatedTuple)) return eval;
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
        //System.out.println("Filter correlated operator closed");

    }
}
