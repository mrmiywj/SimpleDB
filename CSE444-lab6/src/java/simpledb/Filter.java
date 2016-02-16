package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate p;
    private DbIterator child;
    
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.close();
    	super.close();
    	super.open();
    	child.open();
    	
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
    	if (child == null)
    		throw new NoSuchElementException("No iterator");
    	if (!child.hasNext())
    		return null;
    	else
    	{
    		Tuple t = child.next();
    		if (t == null)
    			return null;
    		if (p.filter(t))
    			return t;
    		else
    			return fetchNext();
    	}
        //return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] ret;
    	ret = new DbIterator[1];
    	ret[0] = child;
    	return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	DbIterator i = children[0];
    	child = i;
    }

}
