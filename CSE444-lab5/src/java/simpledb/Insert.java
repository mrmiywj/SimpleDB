package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private boolean fetched = false;
    private TupleDesc td;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	this.tableId = tableid;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        Tuple t = new Tuple(td);
        try{
        	int count = 0;
        	if (fetched)
        		return null;
        	else
        	{
        		fetched = true;
        		while(child.hasNext())
        		{
        			Tuple tmp = child.next();
        			try{
        				Database.getBufferPool().insertTuple(tid, tableId, tmp);
        			}
        			catch (IOException e)
        			{
        				e.printStackTrace();
        			}
        			++count;
        		}
        	}
        	Field fd = new IntField(count);
        	t.setField(0, fd);
        }
        catch (DbException e)
        {
        	e.printStackTrace();
        }
        catch (TransactionAbortedException e)
        {
        	e.printStackTrace();
        }
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
    	this.child = children[0];
    }
}
