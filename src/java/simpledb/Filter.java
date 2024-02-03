package simpledb;

import org.w3c.dom.ls.LSInput;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    // Fields
    private final Predicate predicate;
    private OpIterator child;
    private TupleDesc tupleDesc;
    private List<Tuple> childList;
    private Iterator<Tuple> tupleIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.childList = new ArrayList<>();
        while (this.child.hasNext())
            this.childList.add(this.child.next());
        this.tupleIterator = this.childList.iterator();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.childList = null;
        this.tupleIterator = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.childList != null)
            this.tupleIterator = this.childList.iterator();
        // not sure if operators need to recursively call rewind() on child operators
        // this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        // scan through the tupleIterator and return only a tuple that pass the predicate
        while (this.tupleIterator != null && this.tupleIterator.hasNext()) {
            Tuple nextTuple = this.tupleIterator.next();
            if (this.predicate.filter(nextTuple))
                return nextTuple;
        }
        return null;
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
