package simpledb.parallel;

import simpledb.Field;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The default implementation of the partition function.
 * 
 * The partition of a tuple is decided by the hash code
 * of a preset field of the tuple.
 * */
public class SingleFieldHashPartitionFunction extends
	PartitionFunction<String, String> {

    private static final long serialVersionUID = 1L;

    public SingleFieldHashPartitionFunction(int numPartition) {
        super(numPartition);
    }

    public static final String FIELD_NAME = "field_name";
    private String fieldName = null;

    /**
     * This partition function only needs the index of the
     * partition field in deciding the tuple partitions
     * */
    @Override
    public void setAttribute(String attribute, String value) {
        super.setAttribute(attribute, value);
        if (attribute.equals(FIELD_NAME))
            this.fieldName = value;
    }

    @Override
    public int partition(Tuple t, TupleDesc td) {
        int fieldIndex = td.fieldNameToIndex(this.fieldName);
	Field f = t.getField(fieldIndex);
	int remain = f.hashCode() % numPartition();
	if (remain < 0)
	    remain += numPartition();
	return remain;
    }

}
