package simpledb.parallel;

import java.util.BitSet;
import java.util.HashSet;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.Field;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.parallel.Exchange.ParallelOperatorID;

/**
 * The bloom filter producer
 * */
public class BloomFilterProducer extends Producer {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private BitSet bloomFilter;
    private int filterField;
    private SocketInfo[] workers;
    private int size;

    public static int[] primeNumbers = new int[] { 2, 3, 5, 7, 11, 13, 17, 19,
            23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
            101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163,
            167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233,
            239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311,
            313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389,
            397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463,
            467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563,
            569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641,
            643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727,
            733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821,
            823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907,
            911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997 };

    public static int[] sampleSubsetPrime(int size) {
        int maxSize = primeNumbers.length / 2;
        if (size > maxSize)
            size = maxSize;

        HashSet<Integer> primes = new HashSet<Integer>();

        while (primes.size() < size) {
            int v = (int) (Math.random() * primeNumbers.length);
            primes.add(v);
        }

        int[] result = new int[size];
        int idx = 0;
        for (Integer k : primes)
            result[idx++] = k;
        return result;
    }

    private int[] primesUsed;

    public BloomFilterProducer(DbIterator child,
            ParallelOperatorID operatorID, SocketInfo[] workers,
            int filterField, int[] primes, int size) {
        super(operatorID);
        this.child = child;
        this.bloomFilter = new BitSet();
        this.filterField = filterField;
        this.primesUsed = primes;
        this.size = size;
        this.workers = workers;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (this.child.hasNext()) {
            Tuple c = this.child.next();
            Field v = c.getField(filterField);
            for (int p : primesUsed) {
                int h = v.hashCode() * p % size;
                if (h < 0)
                    h += size;
                this.bloomFilter.set(h);
            }
            return c;
        }

        ParallelUtility.broadcastMessageToWorkers(new BloomFilterBitSet(
                this.operatorID, this.getThisWorker().workerID, this.bloomFilter),
                workers, getThisWorker().minaHandler,-1);

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
    
    public String getName()
    {
        return "bloom_p";
    }
}

class BloomFilterBitSet extends ExchangeMessage {
    private static final long serialVersionUID = 1L;
    private BitSet bitSet;

    BloomFilterBitSet(ParallelOperatorID oid, String workerID, BitSet bitSet) {
        super(oid,workerID);
        this.bitSet = bitSet;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

}