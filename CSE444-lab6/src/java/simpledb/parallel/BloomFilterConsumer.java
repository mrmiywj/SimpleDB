package simpledb.parallel;

import java.util.BitSet;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The bloom filter consumer
 * */
public class BloomFilterConsumer extends Consumer {

    private static final long serialVersionUID = 1L;

    private final ParallelOperatorID operatorID;
    private final SocketInfo[] sourceWorkers;
    private DbIterator child;
    private final BitSet bloomFilter;
    private volatile int numReceived = 0;
    private int[] primesUsed = null;
    private int filterField = -1;
    private int size;

    class BloomFilterCollector extends Thread {
        public void run() {
            while (numReceived < sourceWorkers.length) {
                try {
                    BloomFilterConsumer.this.bloomFilter.or(((BloomFilterBitSet)(BloomFilterConsumer.this.take(-1))).getBitSet());
                    numReceived += 1;
                    System.out.println("Received "+numReceived+" bitsets");
                } catch (InterruptedException e) {
                }
            }
        }
    }
    
    public String getName()
    {
        return "bloom_c";
    }

    BloomFilterConsumer(DbIterator child, ParallelOperatorID operatorID,
            SocketInfo[] workers, int filterField, int[] indexOfPrimeToUse,int size) {
        super(operatorID);
        this.child = child;
        this.sourceWorkers = workers;
        this.operatorID = operatorID;
        this.bloomFilter = new BitSet();
        this.primesUsed = indexOfPrimeToUse;
        this.filterField = filterField;
        this.size = size;
    }

    public ParallelOperatorID getOperatorID() {
        return this.operatorID;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    protected void buildBloomFilter() {
        if (this.numReceived < this.sourceWorkers.length) {
            Thread collector = new BloomFilterCollector();
            collector.start();
            try {
                collector.join();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    public void close() {
        super.close();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    protected boolean filter(Tuple t) {
        
        for (int p : this.primesUsed) {
            int h = t.getField(this.filterField).hashCode() * p % size;
            if (h<0)
                h+=size;
            
            if (!this.bloomFilter.get(h))
                return false;
        }
        return true;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        buildBloomFilter();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (filter(t))
                return t;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
