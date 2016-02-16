package simpledb.parallel;

import java.io.Serializable;
import java.util.HashMap;

import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to
 * decide which worker a tuple should be routed to. Typically, the
 * ShuffleProducer class invokes {@link partition(Tuple, TupleDesc) partition}
 * on every tuple it generates.
 * */
public abstract class PartitionFunction<K, V> implements Serializable {

    private static final long serialVersionUID = 1L;

    HashMap<K, V> attributes = new HashMap<K, V>();
    int numPartition;

    /**
     * A concrete implementation of a partition function may need some
     * information to help it decide the tuple partitions.
     * */
    public void setAttribute(K attribute, V value) {
        this.attributes.put(attribute, value);
    }

    public V getAttribute(K attribute) {
        return attributes.get(attribute);
    }

    /**
     * Each partition function implementation must has a Class(int) style
     * constructor
     * */
    public PartitionFunction(int numPartition) {
        this.numPartition = numPartition;
    }

    public int numPartition() {
        return this.numPartition;
    }

    /**
     * Given an input tuple t, determine which partition to route it to.
     * 
     * Note: TupleDesc td is explicitly required even though the Tuple t
     * includes a TupleDesc (obtained by calling t.getTupleDesc()) since field
     * names might be absent from t.getTupleDesc(), and the PartitionFunction
     * might require field names.
     * 
     * 
     * 
     * @param t
     *            the input tuple to route.
     * @param td
     *            the tuple descriptor of the input tuple. Must have non-null
     *            names for those attributes that are used to compute the worker
     *            to route to.
     * 
     * @return the worker to send the tuple to.
     * 
     * */
    public abstract int partition(Tuple t, TupleDesc td);

}
