package simpledb.parallel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The consumer part of the Collect Exchange operator.
 * 
 * A Collect operator collects tuples from all the workers.
 * There is a collect producer on each worker, and a collect
 * consumer on the server and a master worker if a master worker
 * is needed.
 * 
 * The consumer passively collects Tuples from all the paired
 * CollectProducers
 * 
 * */
public class CollectConsumer extends Consumer {

    private static final long serialVersionUID = 1L;

    private transient Iterator<Tuple> tuples;
    
    /**
     * innerBufferIndex and innerBuffer are used to buffer
     * all the TupleBags this operator has received. We need
     * this because we need to support rewind.
     * */
    private transient int innerBufferIndex;
    private transient ArrayList<TupleBag> innerBuffer;

    private TupleDesc td;
    private final BitSet workerEOS;
    private final SocketInfo[] sourceWorkers;
    private final HashMap<String, Integer> workerIdToIndex;
    /**
     * The child of a CollectConsumer must be a paired CollectProducer.
     * */
    private CollectProducer child;

    public String getName()
    {
        return "collect_c";
    }
    
    /**
     * If there's no child operator, a TupleDesc is needed
     * */
    public CollectConsumer(TupleDesc td, ParallelOperatorID operatorID,
	    SocketInfo[] workers) {
        super(operatorID);
        this.td = td;
        this.sourceWorkers = workers;
        this.workerIdToIndex = new HashMap<String, Integer>();
        int idx = 0;
        for (SocketInfo w : workers) {
            this.workerIdToIndex.put(w.getId(), idx++);
        }
        this.workerEOS = new BitSet(workers.length);
    }

    /**
     * If a child is provided, the TupleDesc is the child's TD
     * */
    public CollectConsumer(CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers) {
        super(operatorID);
        this.child = child;
        this.td = child.getTupleDesc();
        this.sourceWorkers = workers;
        this.workerIdToIndex = new HashMap<String, Integer>();
        int idx = 0;
        for (SocketInfo w : workers) {
            this.workerIdToIndex.put(w.getId(), idx++);
        }
        this.workerEOS = new BitSet(workers.length);
	
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
	this.tuples = null;
	this.innerBuffer = new ArrayList<TupleBag>();
	this.innerBufferIndex = 0;
	if (this.child != null)
	    this.child.open();
	super.open();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
	this.tuples = null;
	this.innerBufferIndex = 0;
    }

    public void close() {
	super.close();
	this.setBuffer(null);
	this.tuples = null;
	this.innerBufferIndex = -1;
	this.innerBuffer = null;
	this.workerEOS.clear();
    }

    @Override
    public TupleDesc getTupleDesc() {
        if (this.child!=null)
            return this.child.getTupleDesc();
        else
            return this.td;
    }

    Iterator<Tuple> getTuples() throws InterruptedException {
	TupleBag tb = null;
	if (this.innerBufferIndex < this.innerBuffer.size())
	    return this.innerBuffer.get(this.innerBufferIndex++).iterator();

	while (this.workerEOS.nextClearBit(0) < this.sourceWorkers.length) {
	    tb = (TupleBag)this.take(-1);
	    if (tb.isEos()) {
		this.workerEOS.set(this.workerIdToIndex.get(tb.getWorkerID()));
	    } else {
		innerBuffer.add(tb);
		this.innerBufferIndex++;
		return tb.iterator();
	    }
	}
	// have received all the eos message from all the workers
	return null;

    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
	while (tuples == null || !tuples.hasNext()) {
	    try {
		this.tuples = getTuples();
	    } catch (InterruptedException e) {
	        e.printStackTrace();
		throw new DbException(e.getLocalizedMessage());
	    }
	    if (tuples == null) // finish
		return null;
	}
	return tuples.next();

    }

    @Override
    public DbIterator[] getChildren() {
	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    this.child = (CollectProducer) children[0];
	    if (this.child != null)
		this.td = this.child.getTupleDesc();
    }

}
