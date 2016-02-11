package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private boolean noGrouping = false;
    private HashMap<Field, Integer> groups;
    private String fieldName = "",groupFieldName = "";
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aField = afield;
    	this.what = what;
    	if (gbfield == Aggregator.NO_GROUPING)
    		this.noGrouping = true;
    	groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key;
    	int curr;
    	fieldName = tup.getTupleDesc().getFieldName(aField);
    	if (noGrouping)
    	{
    		key = new IntField(Aggregator.NO_GROUPING);
    	}
    	else
    	{
    		key = tup.getField(gbField);
    		groupFieldName = tup.getTupleDesc().getFieldName(gbField);
    	}
    	
    	if (groups.containsKey(key))
    	{
    		curr = groups.get(key);
    	}
    	else
    	{
    		groups.put(key, 0);
    	}
    	curr = groups.get(key);
    	++curr;
    	groups.put(key, curr);
    }
    
    public TupleDesc getTupleDesc() {
    	Type[] typeAr;
    	String[] stringAr;
    	TupleDesc td;

    	if (noGrouping) {
    		typeAr = new Type[1];
    		stringAr = new String[1];
    		typeAr[0] = Type.INT_TYPE;
    		stringAr[0] = fieldName;//don't actually need real field name
    	} else {
    		typeAr = new Type[2];
    		stringAr = new String[2];
    		typeAr[0] = gbFieldType;
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td = this.getTupleDesc();
        
        if (noGrouping)
        {
        	for (Field key:groups.keySet())
        	{
        		int value = groups.get(key);
        		Tuple tp = new Tuple(td);
        		tp.setField(0, new IntField(value));
        		tuples.add(tp);
        	}
        }
        else
        {
        	for (Field key:groups.keySet())
        	{
        		int value = groups.get(key);
        		
        		Tuple e = new Tuple(td);
        		e.setField(0, key);
        		e.setField(1, new IntField(value));
        		tuples.add(e);
        	}
        }
        return new TupleIterator(td, tuples);
    }

}
