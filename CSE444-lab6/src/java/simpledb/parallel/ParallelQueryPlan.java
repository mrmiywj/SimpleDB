package simpledb.parallel;

import java.util.HashSet;

import simpledb.parallel.Exchange.ParallelOperatorID;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbIterator;
import simpledb.Filter;
import simpledb.HashEquiJoin;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.OrderBy;
import simpledb.Project;
import simpledb.TransactionId;

/**
 * A wrapper class which wraps the parallel plans of a local query plan.
 * */
public class ParallelQueryPlan {

    private CollectProducer masterWorkerPlan;
    private CollectConsumer serverPlan;
    private boolean needMaster = false;

    /**
     * An optimizer should be conducted only once on a query
     * */
    final HashSet<Class<? extends ParallelQueryPlanOptimizer>> optimizedBy;

    public CollectProducer getMasterWorkerPlan() {
        return this.masterWorkerPlan;
    }

    void setMasterWorkerPlan(CollectProducer masterPlan) {
        this.masterWorkerPlan = masterPlan;
    }

    public CollectConsumer getServerPlan() {
        return this.serverPlan;
    }

    public CollectProducer getSlaveWorkerPlan() {
        if (!this.needMaster)
            return masterWorkerPlan;
        else {
            return (CollectProducer) this
                    .extractSlaveWorkerPlan(this.masterWorkerPlan.getChildren()[0]);
        }
    }

    private DbIterator extractSlaveWorkerPlan(DbIterator root) {
        if (root instanceof CollectProducer)
            return root;
        else
            return extractSlaveWorkerPlan(((Operator) root).getChildren()[0]);
    }

    private ParallelQueryPlan() {
        optimizedBy = new HashSet<Class<? extends ParallelQueryPlanOptimizer>>();
    }

    /**
     * Insert shuffle producer/consumer and collect producer/consumer operators
     * into the local query plan.
     * 
     * Note that this method will directly modify the localQueryPlan
     * */
    public static ParallelQueryPlan parallelizeQueryPlan(TransactionId tId,
            DbIterator localQueryPlan, SocketInfo[] workers,
            SocketInfo masterWorker, SocketInfo server,
            Class<? extends PartitionFunction<?, ?>> partitionFunction) {
        ParallelQueryPlan parallelPlan = new ParallelQueryPlan();
        ParallelOperatorID serverCollectOID = ParallelOperatorID.newID(tId);

        QueryPlanWrapper slavePlanWrapper = null;
        try {
            slavePlanWrapper = buildSlaveWorkerPlan(localQueryPlan, tId,
                    SingleFieldHashPartitionFunction.class, workers,
                    masterWorker);
        } catch (Exception e) {
            e.printStackTrace();
        }

        DbIterator masterPlan = null;

        if (slavePlanWrapper.collectorOperator != null) {// need a master worker
            parallelPlan.needMaster = true;
            DbIterator master = buildMasterWorkerPlan(tId, localQueryPlan,
                    slavePlanWrapper, workers);
            masterPlan = new CollectProducer(master, serverCollectOID,
                    server.getAddress());
            parallelPlan.serverPlan = new CollectConsumer(
                    localQueryPlan.getTupleDesc(), serverCollectOID,
                    new SocketInfo[] { masterWorker });
        } else {
            DbIterator tmp = slavePlanWrapper.queryPlan;
            slavePlanWrapper.queryPlan = new CollectProducer(tmp,
                    serverCollectOID, server.getAddress());
            masterPlan = slavePlanWrapper.queryPlan;
            parallelPlan.serverPlan = new CollectConsumer(
                    localQueryPlan.getTupleDesc(), serverCollectOID, workers);
        }

        parallelPlan.masterWorkerPlan = (CollectProducer) masterPlan;

        return parallelPlan;
    }

    protected static class QueryPlanWrapper {
        DbIterator queryPlan;
        DbIterator collectorOperator;

        QueryPlanWrapper(DbIterator queryPlan, DbIterator collector) {
            this.queryPlan = queryPlan;
            this.collectorOperator = collector;
        }

        QueryPlanWrapper(DbIterator queryPlan) {
            this.queryPlan = queryPlan;
            this.collectorOperator = null;
        }
    }

    protected static DbIterator buildMasterWorkerPlan(TransactionId tId,
            DbIterator root, QueryPlanWrapper slavePlanWrapper,
            SocketInfo[] sourceWorkers) {
        if (slavePlanWrapper.collectorOperator == null)
            return slavePlanWrapper.queryPlan;

        DbIterator[] children = new DbIterator[] {};
        if (root instanceof Operator)
            children = ((Operator) root).getChildren();

        if (root == slavePlanWrapper.collectorOperator) {
            if (root instanceof Aggregate || root instanceof OrderBy) {
                CollectProducer slavePlan = (CollectProducer) slavePlanWrapper.queryPlan;
                CollectConsumer c = new CollectConsumer(slavePlan,
                        slavePlan.getOperatorID(), sourceWorkers);
                ((Operator) root).setChildren(new DbIterator[] { c });
                return root;
            }
            throw new IllegalStateException(
                    "Unknown collector operator. Only aggregate and order by are possible");
        } else {
            children[0] = buildMasterWorkerPlan(tId, children[0],
                    slavePlanWrapper, sourceWorkers);
            ((Operator) root).setChildren(children);
            return root;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static QueryPlanWrapper buildSlaveWorkerPlan(DbIterator root,
            TransactionId tId, Class<? extends PartitionFunction<?, ?>> pc,
            SocketInfo[] workers, SocketInfo masterWorker) throws Exception {
        DbIterator[] children = new DbIterator[] {};
        if (root instanceof Operator)
            children = ((Operator) root).getChildren();

        if (root instanceof HashEquiJoin || root instanceof Join) {

            JoinPredicate jp = null;
            if (root instanceof HashEquiJoin)
                jp = ((HashEquiJoin) root).getJoinPredicate();
            else if (root instanceof Join)
                jp = ((Join) root).getJoinPredicate();

            QueryPlanWrapper child1 = buildSlaveWorkerPlan(children[0], tId,
                    pc, workers, masterWorker);
            QueryPlanWrapper child2 = buildSlaveWorkerPlan(children[1], tId,
                    pc, workers, masterWorker);

            if (child1.collectorOperator != null
                    || child2.collectorOperator != null)
                throw new IllegalStateException(
                        "Sub queries are not allowed. It should be impossible to reach here");

            ParallelOperatorID oid1 = ParallelOperatorID.newID(tId);
            PartitionFunction p1 = pc.getConstructor(Integer.TYPE).newInstance(
                    workers.length);
            p1.setAttribute(SingleFieldHashPartitionFunction.FIELD_NAME,
                    child1.queryPlan.getTupleDesc()
                            .getFieldName(jp.getField1()));

            ShuffleProducer s12 = new ShuffleProducer(child1.queryPlan, oid1,
                    workers, p1);
            ShuffleConsumer s11 = new ShuffleConsumer(s12, oid1, workers);

            ParallelOperatorID oid2 = ParallelOperatorID.newID(tId);
            PartitionFunction p2 = pc.getConstructor(Integer.TYPE).newInstance(
                    workers.length);
            p2.setAttribute(SingleFieldHashPartitionFunction.FIELD_NAME,
                    child2.queryPlan.getTupleDesc()
                            .getFieldName(jp.getField2()));

            ShuffleProducer s22 = new ShuffleProducer(child2.queryPlan, oid2,
                    workers, p2);
            ShuffleConsumer s21 = new ShuffleConsumer(s22, oid2, workers);

            ((Operator) root).setChildren(new DbIterator[] { s11, s21 });
            return new QueryPlanWrapper(root);
        } else if (root instanceof Aggregate) {
            Aggregate a = (Aggregate) root;

            QueryPlanWrapper child = buildSlaveWorkerPlan(children[0], tId, pc,
                    workers, masterWorker);

            if (child.collectorOperator != null)
                // TODO: Re shuffle if the aggregate is a group by
                return child;
            else {
                if (a.groupField() == Aggregator.NO_GROUPING) {
                    CollectProducer c = new CollectProducer(child.queryPlan,
                            ParallelOperatorID.newID(tId),
                            masterWorker.getAddress());
                    child.queryPlan = c;
                    child.collectorOperator = root;
                    return child;
                } else {
                    PartitionFunction p = pc.getConstructor(Integer.TYPE)
                            .newInstance(workers.length);
                    p.setAttribute(SingleFieldHashPartitionFunction.FIELD_NAME,
                            a.groupFieldName());
                    ParallelOperatorID oID = ParallelOperatorID.newID(tId);
                    ShuffleProducer sp = new ShuffleProducer(child.queryPlan,
                            oID, workers, p);
                    ShuffleConsumer sc = new ShuffleConsumer(sp, oID, workers);
                    ((Aggregate) root).setChildren(new DbIterator[] { sc });
                    child.queryPlan = root;
                    return child;
                }
            }

        } else if (root instanceof Filter) {
            Filter f = ((Filter) root);
            QueryPlanWrapper child = buildSlaveWorkerPlan(children[0], tId, pc,
                    workers, masterWorker);
            if (child.collectorOperator != null)
                return child;
            else {
                f.setChildren(new DbIterator[] { child.queryPlan });
                child.queryPlan = f;
                return child;
            }
        } else if (root instanceof OrderBy) {
            QueryPlanWrapper child = buildSlaveWorkerPlan(children[0], tId, pc,
                    workers, masterWorker);
            if (child.collectorOperator != null)
                return child;
            else {
                CollectProducer c = new CollectProducer(child.queryPlan,
                        ParallelOperatorID.newID(tId),
                        masterWorker.getAddress());
                child.queryPlan = c;
                child.collectorOperator = root;
                return child;
            }
        } else if (root instanceof Project) {
            Project f = ((Project) root);
            QueryPlanWrapper child = buildSlaveWorkerPlan(children[0], tId, pc,
                    workers, masterWorker);
            if (child.collectorOperator != null)
                return child;
            else {
                f.setChildren(new DbIterator[] { child.queryPlan });
                child.queryPlan = f;
                return child;
            }
        }
        return new QueryPlanWrapper(root);
    }

    public static ParallelQueryPlan optimize(TransactionId tid,
            ParallelQueryPlan unOptimizedPlan,
            ParallelQueryPlanOptimizer optimizer) {
        return optimizer.optimize(tid, unOptimizedPlan);
    }

}
