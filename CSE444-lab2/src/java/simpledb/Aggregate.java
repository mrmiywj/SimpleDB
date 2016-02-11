package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private DbIterator aggregatedChild;
    private int aField;
    private int gField;
    private Aggregator.Op aOp;
    private Aggregator aggre;
    private Type gbFieldType;
    private boolean noGrouping = false;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.aField = afield;
    	this.gField = gfield;
    	this.aOp = aop;
    	if (gfield == Aggregator.NO_GROUPING)
    		gbFieldType = null;
    	else 
    		gbFieldType = child.getTupleDesc().getFieldType(gfield);
    	Type t = child.getTupleDesc().getFieldType(afield);
    	if (gfield == -1)
    		noGrouping = true;
    	if (t == Type.INT_TYPE)
    	{
    		aggre = new IntegerAggregator(gfield, gbFieldType, afield, aop);
    	}
    	if (t == Type.STRING_TYPE)
    	{
    		aggre = new StringAggregator(gfield, gbFieldType, afield, aop);
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	if (noGrouping)
    		return Aggregator.NO_GROUPING;
    	else
    		return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if (noGrouping)
    		return null;
    	else
    		return this.child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return this.child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.aOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	this.child.open();
    	while(child.hasNext())
    		aggre.mergeTupleIntoGroup(child.next());
    	aggregatedChild = aggre.iterator();
    	aggregatedChild.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if (aggregatedChild.hasNext())
    		return aggregatedChild.next();
    	else 
    		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	child.rewind();
    	aggregatedChild.rewind();
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
    	Type[] fieldType;
    	String[] fieldName;
    	
    	if (noGrouping)
    	{
    		fieldType = new Type[1];
    		fieldName = new String[1];
    		
    		fieldType[0] = this.child.getTupleDesc().getFieldType(aField);
    		fieldName[0] = aOp.toString() + "(" + child.getTupleDesc().getFieldName(aField) + ")";
    		return new TupleDesc(fieldType, fieldName);
    	}
    	else
    	{
    		fieldType = new Type[2];
    		fieldName = new String[2];
    		fieldType[0] = child.getTupleDesc().getFieldType(gField);
    		fieldName[0] = child.getTupleDesc().getFieldName(gField);
    		
    		fieldType[1] = this.child.getTupleDesc().getFieldType(aField);
    		fieldName[1] = aOp.toString() + "(" + child.getTupleDesc().getFieldName(aField) + ")";
    		return new TupleDesc(fieldType, fieldName);
    	}
    }

    public void close() {
	// some code goes here
    	super.close();
    	aggregatedChild.close();
    	child.close();
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
