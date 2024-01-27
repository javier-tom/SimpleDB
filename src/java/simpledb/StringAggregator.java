package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Fields
    private int groupByField;
    private Type groupByType;
    private int aggregateField;
    private Op aggregateOp;

    private Map<Field, Integer> groupByMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT)
            throw new IllegalArgumentException();
        this.groupByField = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateOp = what;

        this.groupByMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field currentGroupByField = null;
        if (this.groupByField != Aggregator.NO_GROUPING) {
            currentGroupByField = tup.getField(this.groupByField);
        }
        int count = this.groupByMap.getOrDefault(currentGroupByField, 0);
        this.groupByMap.put(currentGroupByField, count + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private TupleDesc tupleDesc;
            private List<Tuple> aggregateValues;
            private Iterator<Tuple> tupleIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.aggregateValues = new ArrayList<>();
                if (groupByField == Aggregator.NO_GROUPING) {
                    tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
                    Tuple currentTuple = new Tuple(tupleDesc);
                    currentTuple.setField(0, new IntField(1));
                    this.aggregateValues.add(currentTuple);
                } else {
                    tupleDesc = new TupleDesc(new Type[] {groupByType, Type.INT_TYPE});
                    for (Map.Entry<Field, Integer> mapEntry : groupByMap.entrySet()) {
                        Tuple currentTuple = new Tuple(tupleDesc);
                        currentTuple.setField(0, mapEntry.getKey());
                        currentTuple.setField(1, new IntField(mapEntry.getValue()));
                        this.aggregateValues.add(currentTuple);
                    }
                }
                this.tupleIterator = this.aggregateValues.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.tupleIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return this.tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.tupleIterator = this.aggregateValues.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.tupleDesc;
            }

            @Override
            public void close() {
                this.tupleIterator = null;
                this.aggregateValues = null;
                this.tupleDesc = null;
            }
        };
    }

}
