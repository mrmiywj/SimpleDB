package simpledb.parallel;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.Aggregator.Op;
import simpledb.DbIterator;
import simpledb.HashEquiJoin;
import simpledb.Join;
import simpledb.Operator;
import simpledb.Rename;
import simpledb.TransactionId;

/**
 * Optimize aggregate operators of a parallel queryplan.
 * 
 * Each aggregate operator within an un-optimized parallel query plan will be
 * replaced by two aggregate operators, a down-stream aggregate operator and an
 * up-stream aggregate operator.
 * 
 * For example, max(column) will be replace by
 * 
 * max(column) -> (shuffle/collect) -> max(column)
 * 
 * In this way, the amount of data that need to be transmitted though network
 * will be minimized.
 * */
public class AggregateOptimizer extends ParallelQueryPlanOptimizer {

    public AggregateOptimizer() {
        super();
    }

    public AggregateOptimizer(ParallelQueryPlanOptimizer next) {
        super(next);
    }

    @Override
    protected void doMyOptimization(TransactionId tid, ParallelQueryPlan plan) {
        plan.setMasterWorkerPlan((CollectProducer) parallelizeAggregate(plan
                .getMasterWorkerPlan()));
    }

    /**
     * Replace the aggregate operators in the local query plan with parallelized
     * aggregate
     * */
    protected DbIterator parallelizeAggregate(DbIterator root) {

        if (!(root instanceof Operator))
            return root;

        Operator rootO = (Operator) root;
        DbIterator[] children = rootO.getChildren();

        if (rootO instanceof Aggregate) {
            Aggregate agg = (Aggregate) rootO;
            Exchange shuffleConsumerOrCollectConsumer = (Exchange) children[0];
            Exchange shuffleProducerOrCollectProducer = (Exchange) shuffleConsumerOrCollectConsumer
                    .getChildren()[0];

            DbIterator downChildProcessed = parallelizeAggregate(shuffleProducerOrCollectProducer
                    .getChildren()[0]);
            shuffleProducerOrCollectProducer
                    .setChildren(new DbIterator[] { downChildProcessed });

            Op aop = agg.aggregateOp();
            Aggregate downAgg = agg;
            Aggregate upAgg = agg;

            boolean hasGroup = agg.groupField() != Aggregator.NO_GROUPING;

            switch (aop) {
            /**
             * replace AVG with SUM_COUNT -> SC_AVG
             * */
            case AVG:
                downAgg = new Aggregate(downChildProcessed,
                        agg.aggregateField(), agg.groupField(),
                        Aggregator.Op.SUM_COUNT);
                shuffleProducerOrCollectProducer
                        .setChildren(new DbIterator[] { downAgg });
                upAgg = new Aggregate(shuffleConsumerOrCollectConsumer,
                        hasGroup ? 1 : 0,
                        hasGroup ? 0 : Aggregator.NO_GROUPING,
                        Aggregator.Op.SC_AVG);
                break;
            /**
             * replace SUM with SUM -> SUM
             * */
            case SUM:
                /**
                 * replace COUNT with COUNT -> SUM
                 * */
            case COUNT:
                // some code goes here
                break;
            /**
             * replace MIN with MIN -> MIN
             * */
            case MIN:
                // some code goes here
                break;
            /**
             * replace MAX with MAX -> MAX
             * */
            case MAX:
                downAgg.setChildren(new DbIterator[] { downChildProcessed });
                shuffleProducerOrCollectProducer
                        .setChildren(new DbIterator[] { downAgg });
                upAgg = new Aggregate(shuffleConsumerOrCollectConsumer,
                        hasGroup ? 1 : 0,
                        hasGroup ? 0 : Aggregator.NO_GROUPING,
                        Aggregator.Op.MAX);
                break;
            }

            /**
             * Add a rename operator to keep the output TupleDesc consistent
             * with the local plan
             * */
            return new Rename(hasGroup ? 1 : 0, agg.aggregateFieldName(), upAgg);
        } else {
            if (rootO instanceof Join || rootO instanceof HashEquiJoin) {
                DbIterator child1 = parallelizeAggregate(children[0]);
                DbIterator child2 = parallelizeAggregate(children[1]);
                rootO.setChildren(new DbIterator[] { child1, child2 });
                return rootO;
            } else {
                DbIterator child = parallelizeAggregate(children[0]);
                rootO.setChildren(new DbIterator[] { child });
                return rootO;
            }
        }

    }
}
