package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean fetched = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
        Type[] tArr = new Type[1];
        String[] nArr = new String[1];
        tArr[0] = Type.INT_TYPE;
        nArr[0] = "Number of inserted records";
        td = new TupleDesc(tArr, nArr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
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
    	close();
    	open();
}

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
       	Tuple t = new Tuple(td);
       	int count=0;
       	try{
       		if (fetched)
       			return null;
       		fetched = true;
       		while(child.hasNext())
       		{
       			Tuple tup = child.next();
       			Database.getBufferPool().deleteTuple(tid, tup);
       			count++;
       		}
       	}
       	catch (DbException e)
       	{
       		e.printStackTrace();
       	}
       	catch (IOException e)
       	{
       		e.printStackTrace();
       	}
       	Field fd = new IntField(count);
       	t.setField(0, fd);
       	return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
