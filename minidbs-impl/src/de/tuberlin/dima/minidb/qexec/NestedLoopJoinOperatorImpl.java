package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

import java.util.ArrayList;


public class NestedLoopJoinOperatorImpl implements NestedLoopJoinOperator{
    private final PhysicalPlanOperator outerChild;
    private final PhysicalPlanOperator innerChild;
    private final JoinPredicate joinPredicate;
    private final int[] columnMapOuterTuple;
    private final int[] columnMapInnerTuple;

    private DataTuple outerTuple;

    private DataTuple innerTuple;

    /**
     * Creates a new Nested-Loop-Join operator, drawing tuples from the outer side in the outer
     * loop and from the inner side in the inner loop. The inner side is opened and closed for
     * each tuple from the outer side. The join optionally evaluates a join predicate. Optionally
     * is because in some cases, no join predicate exists (Cartesian Join), or the join predicate
     * is already represented in the correlation.
     *
     * The tuples produced by the join operator are (in most cases) a concatenation of
     * the tuple from outer and inner side. How the columns from the output tuple are derived
     * from the columns of the input tuple is described in two map arrays:
     * <tt>columnMapOuterTuple</tt> and <tt>columnMapInnerTuple</tt>. At position <tt>i</tt> in
     * such a map array is the position of the column in the outer (respectively inner) tuple that
     * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
     * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
     * outer tuple (respectively inner) tuple.
     *
     * Here is an example of how to assign the fields of the outer tuple to the output tuple:
     * <code>
     * for (int i = 0; i < outerTupleColumnMap.length; i++) {
     *     int index = outerTupleColumnMap[i];
     *     if (index != -1) {
     *         outputTuple.assignDataField(currentOuterTuple.getField(index), i);
     *     }
     * }
     * </code>
     *
     * @param outerChild The outer child for the nested-loop-join, whose tuple are pulled in the
     *                   outer loop.
     * @param innerChild The inner child for the nested-loop-join, whose tuples are pulled in the
     *                   inner loop.
     * @param joinPredicate The join predicate to be evaluated on the tuples. The predicate is
     *                      constructed such that the tuples from the outer child are the left hand
     *                      side argument and the tuples from the inner child are the right hand side
     *                      argument.
     * @param columnMapOuterTuple The map describing how the columns from the outer tuple are copied
     *                            to the output tuple. See also above column.
     * @param columnMapInnerTuple The map describing how the columns from the inner tuple are copied
     *                            to the output tuple. See also above column.
     */
    public NestedLoopJoinOperatorImpl(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate, int[] columnMapOuterTuple, int[] columnMapInnerTuple) {
        this.outerChild = outerChild;
        this.innerChild = innerChild;
        this.joinPredicate = joinPredicate;
        this.columnMapOuterTuple = columnMapOuterTuple;
        this.columnMapInnerTuple = columnMapInnerTuple;
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
        outerChild.open(correlatedTuple);
        outerTuple = outerChild.next();

        innerChild.open(outerTuple);
        innerTuple = innerChild.next();
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

        // Use joinPredicate.evaluate() on the current outer and inner tuples.
        while (outerTuple != null){
            while (innerTuple != null){
                // Case 1: Join predicate is already evaluated through the correlated tuple
                if(joinPredicate == null){
                    return mergeTuples(innerTuple, columnMapInnerTuple, outerTuple, columnMapOuterTuple);
                }
                // Case 2: Inner child does not evaluate the correlated tuple, e.g., a TableScanOperator.
                if (joinPredicate.evaluate(outerTuple, innerTuple)){
                    return mergeTuples(innerTuple, columnMapInnerTuple, outerTuple, columnMapOuterTuple);
                }
                innerTuple = innerChild.next();
            }
            innerChild.close();
            outerTuple = outerChild.next();
            if (outerTuple != null){
                innerChild.open(outerTuple);
                innerTuple = innerChild.next();
            }
        }
        return null;
    }

    public DataTuple mergeTuples(DataTuple leftTuple, int[] columnMapInnerTuple, DataTuple rightTuple,
                                 int[] columnMapOuterTuple) throws QueryExecutionException {
        // Use column maps to output projection of pair outerTuple and leftTuple
        // Create new joined tuple
        DataTuple joinedTuple = new DataTuple(columnMapOuterTuple.length);
        // Assign to the joined tuple the outer fields
        for (int i = 0; i < columnMapOuterTuple.length; i++) {
            int columnOuterTuple = columnMapOuterTuple[i];
            // A value other than -1 indicates that the corresponding field of the input tuple should be
            // used as the field in the output tuple that corresponds to the array position.
            if(columnOuterTuple != -1){
                DataField outerField = rightTuple.getField(columnOuterTuple);
                joinedTuple.assignDataField(outerField, i);
            }
            // A value -1 indicates that the attribute of the other input tuple is used
            else {
                DataField innerField = leftTuple.getField(columnMapInnerTuple[i]);
                joinedTuple.assignDataField(innerField, i);
            }
        }
        innerTuple = innerChild.next();
        return joinedTuple;
    }

    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    @Override
    public void close() throws QueryExecutionException {
        innerChild.close();
        outerChild.close();
    }
}
