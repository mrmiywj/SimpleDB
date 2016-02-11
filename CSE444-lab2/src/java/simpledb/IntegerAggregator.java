package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.xml.Log4jEntityResolver;

import com.sun.istack.internal.logging.Logger;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private boolean no_group = false;
    private HashMap<Field, Integer> groups;
    private HashMap<Field, Integer> counts;
    private String fieldName = "";
    private String groupFieldName = "";
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if (gbfield==Aggregator.NO_GROUPING) {
    		no_group=true;
    	}
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	groups = new HashMap<Field, Integer>();
    	counts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key;
    	int value;
    	int currentAggregateValue;
    	int currentCount;
    	java.util.logging.Logger.getGlobal().info(tup.toString());
    	fieldName = tup.getTupleDesc().getFieldName(afield);
    	if (no_group)
    	{
    		key = new IntField(Aggregator.NO_GROUPING);
    	}
    	else
    	{
    		key = tup.getField(gbfield);
    		fieldName = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	value = ((IntField)tup.getField(afield)).getValue();
    	if (counts.containsKey(key))
    	{
    		currentCount = counts.get(key);
    	}
    	
    	if (groups.containsKey(key))
    	{
    		currentAggregateValue = groups.get(key);
    	}
    	else
    	{
    		if (what == Op.COUNT || what == Op.SUM || what == Op.AVG)
    		{
    			groups.put(key, 0);
    			counts.put(key, 0);
    		}
    		else if (what == Op.MAX)
    		{
    			groups.put(key, Integer.MIN_VALUE);
    			counts.put(key, 0);
    		}
    		else if (what == Op.MIN)
    		{
    			groups.put(key, Integer.MAX_VALUE);
    			counts.put(key, 0);
    		}
    	}
    	
    	currentAggregateValue = groups.get(key);
    	currentCount = counts.get(key);
    	
    	if (what == Op.SUM)
    	{
    		currentAggregateValue += value;
    		groups.put(key, currentAggregateValue);
    	}
    	if (what == Op.MIN)
    	{
    		if (currentAggregateValue > value)
    		{
    			currentAggregateValue = value;
    			groups.put(key, value);
    		}
    	}
    	if (what == Op.MAX)
    	{
    		if (currentAggregateValue < value)
    		{
    			currentAggregateValue = value;
    			groups.put(key, value);
    		}
    	}
    	if (what == Op.COUNT)
    	{
    		currentAggregateValue++;
    		groups.put(key,value);
    	}
    	if (what == Op.AVG)
    	{
    		currentCount++;
    		counts.put(key, currentCount);
    		currentAggregateValue += value;
    		groups.put(key, currentAggregateValue);
    	}
    }

    public TupleDesc getTupleDesc() {
    	Type[] typeAr;
    	String[] stringAr;
    	TupleDesc td;

    	if (no_group) {
    		typeAr = new Type[1];
    		stringAr = new String[1];
    		typeAr[0] = Type.INT_TYPE;
    		stringAr[0] = fieldName;//don't actually need real field name
    	} else {
    		typeAr = new Type[2];
    		stringAr = new String[2];
    		typeAr[0] = gbfieldtype;
    		typeAr[1] = Type.INT_TYPE;
    		stringAr[0] = groupFieldName;
    		stringAr[1] = fieldName;
    	}
    	td = new TupleDesc(typeAr, stringAr);
    	return td;
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td = this.getTupleDesc();
        
        if (no_group)
        {
    		for (Field key : groups.keySet()){
    			int value = groups.get(key);
    			if (what == Op.AVG) {
    				value = value/counts.get(key);
    			}
    			Tuple tuple = new Tuple(td);
    			tuple.setField(0, new IntField(value));
    			tuples.add(tuple);
    		}
        }
        else
        {
        	for (Field key: groups.keySet())
        	{
        		int value = groups.get(key);
        		if (what == Op.AVG)
        			value = value / counts.get(key);
        		Tuple t = new Tuple(td);
        		t.setField(0, key);
        		t.setField(1, new IntField(value));
        		tuples.add(t);
        	}
        }
        return new TupleIterator(td, tuples);
    }

}
