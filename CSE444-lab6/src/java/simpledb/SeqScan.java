package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator it;
    private DbFile dbf;
    boolean isOpen;
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	dbf = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	isOpen = false;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	it = dbf.iterator(tid);
    	it.open();
    	isOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td = this.dbf.getTupleDesc();
    	int l = td.numFields();
    	Type[] tarr = new Type[l];
    	String[] farr = new String[l];
    	for (int i = 0;i < l; ++i)
    	{
    		tarr[i] = td.getFieldType(i);
    		farr[i] = this.tableAlias + "." + td.getFieldName(i);
    	}
        return new TupleDesc(tarr,farr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (!isOpen)
    		throw new IllegalStateException();
    	if (it == null)
    		return false;
    	else
    		return it.hasNext();
        //return false;
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if (!isOpen)
    		throw new IllegalStateException();
    	if (it == null)
    		throw new IllegalStateException();
    	else
    	{
    		Tuple t = it.next();
    		if (t == null)
    			throw new NoSuchElementException("No next tuple~");
    		else
    			return t;
    	}
    		//return it.next();
       // return null;
    }

    public void close() {
        // some code goes here
    	it = null;
    	isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	it.close();
    	it.open();
    }
}
