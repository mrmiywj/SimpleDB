package simpledb.parallel;

import java.util.HashMap;
import java.util.Map;

import simpledb.DbIterator;
import simpledb.parallel.Exchange.ParallelOperatorID;
import simpledb.HashEquiJoin;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.OperatorCardinality;
import simpledb.SeqScan;
import simpledb.TableStats;
import simpledb.TransactionId;

/**
 *  Add bloom filters to query plan. Sometimes, a join can not have filters spread over from
 *  other join keys. But yet, we know that in the result, there should be very little tuples.
 *  We can use BloomFilter to implement more sophiscated filtering.
 *  
 *  For example:
 * 
 *    select * from actor,casts,movie,genre where movie.id=genre.mid and actor.id=casts.pid
 *    and movie.id=casts.mid and movie.id<2;
 *    
 *    Now we can spread filters to genre.mid<2 and casts.mid<2 but for actor, we still have
 *    to broadcast its tuples to the workers. But since casts.mid has very high selectivity,
 *    casts.aid in the result will also remain just a few.
 *    
 *    Bloom filters can be used here. We can create a bloom filter for casts.aid, and use the
 *    bloom filter to filter actor.id.
 *    
 *    But the problem is we need to get all the casts.aid before we filter actor.id. If the cost
 *    is too much, it is not a good choice to use bloom filter. In this simple implementation, we do
 *    not consider this. 
 *    
 * */
public class BloomFilterOptimizer extends ParallelQueryPlanOptimizer {

    private SocketInfo[] workers = null;
    private HashMap<String,Integer> aliasToIdMapping =null;
    private Map<String,TableStats> tableStats=null;
    
    public BloomFilterOptimizer(SocketInfo[] workers, HashMap<String,Integer> aliasToIdMapping, Map<String,TableStats> tableStats) {
        super();
        this.workers = workers;
        this.aliasToIdMapping = aliasToIdMapping;
        this.tableStats = tableStats;
    }

    public BloomFilterOptimizer(ParallelQueryPlanOptimizer next) {
        super(next);
    }

    @Override
    protected void doMyOptimization(TransactionId tid,ParallelQueryPlan plan) {
        insertBloomFilter(tid,plan);
    }
 
    /**
     * add bloom filters
     * */
    private void insertBloomFilter(TransactionId tid,ParallelQueryPlan plan) {
        CollectProducer masterWorkerPlan = plan.getMasterWorkerPlan();
        OperatorCardinality.updateOperatorCardinality(masterWorkerPlan, aliasToIdMapping, tableStats);
        plan.setMasterWorkerPlan((CollectProducer) doInsertBloomFilter(tid,
                masterWorkerPlan, null));
    }

    private DbIterator doInsertBloomFilter(TransactionId tid,DbIterator root,
            Exchange bloomFilterToInsert) {
        if (root == null)
            return root;

        DbIterator[] children = null;
        if (root instanceof Operator)
            children = ((Operator) root).getChildren();
        else
            return root;

        Operator rootO = (Operator) root;

        if (rootO instanceof Join || rootO instanceof HashEquiJoin) {
            JoinPredicate jp = null;
            if (rootO instanceof Join)
                jp = ((Join) rootO).getJoinPredicate();
            if (rootO instanceof HashEquiJoin)
                jp = ((HashEquiJoin) rootO).getJoinPredicate();

            DbIterator child1 = children[0];
            int child1Card = 1;
            DbIterator child2 = children[1];
            int child2Card = 1;
            if (child1 instanceof SeqScan)
                child1Card = this.tableStats.get(
                        ((SeqScan) child1).getTableName())
                        .estimateTableCardinality(1.0);
            else if (child1 instanceof Operator)
                child1Card = ((Operator) child1).getEstimatedCardinality();
            if (child2 instanceof SeqScan)
                child2Card = this.tableStats.get(
                        ((SeqScan) child2).getTableName())
                        .estimateTableCardinality(1.0);
            else if (child2 instanceof Operator)
                child2Card = ((Operator) child2).getEstimatedCardinality();

            int child1DataSize = child1Card * child1.getTupleDesc().getSize();
            int child2DataSize = child2Card * child2.getTupleDesc().getSize();
            int max = Math.max(child1DataSize, child2DataSize);
            int min = Math.min(child1DataSize, child2DataSize);

            if (min <= 0)
                min = 1;
            if (max > 1024 * 1024 && max * 1.0 / min > 5) {// insert bloom
                                                           // filter
                //should evaluate the cost
                int[] primes = BloomFilterProducer.sampleSubsetPrime(64);
                BloomFilterProducer bfp = null;
                BloomFilterConsumer bfc = null;
                ParallelOperatorID pid = ParallelOperatorID.newID(tid);

                if (max == child1DataSize) {
                    int size = Math.min(child2Card, 2048);
                    bfp = new BloomFilterProducer(null,
                            pid, workers, jp.getField2(), primes, size);
                    children[1] = doInsertBloomFilter(tid,child2, bfp);
                    bfc = new BloomFilterConsumer(null,
                            pid, workers, jp.getField1(), primes, size);
                    children[0] = doInsertBloomFilter(tid,child1, bfc);
                } else {
                    int size = Math.min(child1Card, 2048);
                    bfp = new BloomFilterProducer(null,
                            pid, workers, jp.getField1(), primes, Math.min(
                                    child1Card, 2048));
                    children[0] = doInsertBloomFilter(tid,child1, bfp);

                    bfc = new BloomFilterConsumer(null,
                            pid, workers, jp.getField2(), primes, size);
                    children[1] = doInsertBloomFilter(tid,child2, bfc);
                }
                rootO.setChildren(children);
                return rootO;
            }
        } else if (rootO instanceof ShuffleProducer) {
            if (bloomFilterToInsert != null) {
                bloomFilterToInsert
                        .setChildren(new DbIterator[] { doInsertBloomFilter(tid,
                                children[0], null) });
                rootO.setChildren(new DbIterator[] { bloomFilterToInsert });
                return rootO;
            }
        }
       
        if (children != null)
            for (int i = 0; i < children.length; i++) {
                DbIterator child = children[i];
                if (child != null)
                    children[i] = doInsertBloomFilter(tid,child,
                            bloomFilterToInsert);
            }

        rootO.setChildren(children);
        return rootO;
    }
}
