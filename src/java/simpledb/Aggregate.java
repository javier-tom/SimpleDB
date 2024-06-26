package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // Fields
    private OpIterator child;
    private int aggregateField;
    private int groupByField;
    private Aggregator.Op aggregateOp;

    private final Aggregator aggregator;
    private OpIterator aggregateIterator;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.aggregateField = afield;
        this.groupByField = gfield;
        this.aggregateOp = aop;

        TupleDesc childTupleDesc = child.getTupleDesc();
        Type aggregateFieldType = childTupleDesc.getFieldType(afield);
        Type groupByFieldType = null;
        String groupByName = null;
        if (gfield != Aggregator.NO_GROUPING) {
            groupByFieldType = childTupleDesc.getFieldType(gfield);
            groupByName = childTupleDesc.getFieldName(gfield);
            String[] fieldNames = new String[] {groupByName,
                    aop.toString() + " " + childTupleDesc.getFieldName(afield)};
            this.tupleDesc = new TupleDesc(new Type[] {groupByFieldType, Type.INT_TYPE}, fieldNames);
        } else {
            String[] fieldName = new String[] {aop.toString() + " " + childTupleDesc.getFieldName(afield)};
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, fieldName);
        }

        if (aggregateFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, groupByFieldType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(gfield, groupByFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return this.groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (this.groupByField != Aggregator.NO_GROUPING)
            return this.tupleDesc.getFieldName(0);
	    return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return this.aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        if (this.aggregateField == Aggregator.NO_GROUPING)
            return this.tupleDesc.getFieldName(0);
	    return this.tupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return this.aggregateOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        this.child.open();
        while (this.child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregateIterator = aggregator.iterator();
        aggregateIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (aggregateIterator.hasNext())
            return aggregateIterator.next();
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        aggregateIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	    return this.tupleDesc;
    }

    public void close() {
	// some code goes here
        super.close();
        aggregateIterator.close();
        aggregateIterator = null;
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        this.child = children[0];
    }
    
}
