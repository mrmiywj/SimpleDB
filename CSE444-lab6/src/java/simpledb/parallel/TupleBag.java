package simpledb.parallel;

import java.util.Iterator;

import simpledb.Tuple;
import simpledb.parallel.Exchange.ParallelOperatorID;
import simpledb.TupleDesc;

/**
 * This class wraps a set of tuples. It is the unit of tuple data transmission
 * between exchange operators.
 * */
public class TupleBag extends ExchangeMessage {

    private static final long serialVersionUID = 1L;

    /**
     * Each Producer operator will accumulate tuples and then send the tuples to
     * a corresponding producer operator. As the tuples accumulating, we need to
     * decide when is the good time to wrap up a bunch of tuples and start a
     * round of data transmission.
     * 
     * The strategy we take is as follows: If within MAX_MS, >=MAX_SIZE tuples
     * are accumulated then wrap up the tuples and send them else if we have
     * accumulated for >=MAX_MS if #tuples>=MIN_SIZE then wrap up the tuples and
     * send them else keep accumulating, until at least MIN_SIZE of tuples are
     * accumulated
     * */
    public static final int MAX_SIZE = 2048;

    /**
     * The min number of tuples in a tuple bag.
     * */
    public static final int MIN_SIZE = 16;

    /**
     * The max duration between two data transmissions Unit: milliseconds
     * */
    public static final long MAX_MS = 500;

    /**
     * The tuple data wrapped in the tuplebag.
     * If EOS, tuples is null. 
     * */
    private Tuple[] tuples;
    
    private TupleDesc tupleDesc;

    /**
     * An empty tuple bag means the EOS of the operator
     * */
    public TupleBag(ParallelOperatorID operatorID, String workerID) {
        this(operatorID, workerID, null,null);
    }

    public TupleBag(ParallelOperatorID operatorID, String workerID,
            Tuple[] tuples, TupleDesc tupleDesc) {
        super(operatorID, workerID);
        this.tuples = tuples;
        this.tupleDesc=tupleDesc;
    }

    public boolean isEos() {
        return this.tuples == null;
    }

    public Iterator<Tuple> iterator() {
        return new TupleBagIterator();
    }
    
    public int numTuples()
    {
        if (this.tuples==null)
            return 0;
        return this.tuples.length;
    }
    
    public TupleDesc getTupleDesc()
    {
        return this.tupleDesc;
    }

    class TupleBagIterator implements Iterator<Tuple> {
        int index = 0;

        @Override
        public boolean hasNext() {
            return index < TupleBag.this.tuples.length;
        }

        @Override
        public Tuple next() {
            index += 1;
            Tuple result=TupleBag.this.tuples[index - 1];
            result.resetTupleDesc(TupleBag.this.tupleDesc);
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove unsupported");
        }

    }
}
