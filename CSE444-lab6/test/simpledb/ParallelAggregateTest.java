package simpledb;

import junit.framework.Assert;

import org.junit.Test;

import simpledb.parallel.AggregateOptimizer;
import simpledb.parallel.CollectConsumer;
import simpledb.parallel.CollectProducer;
import simpledb.parallel.ParallelQueryPlan;
import simpledb.parallel.PartitionFunction;
import simpledb.parallel.ShuffleConsumer;
import simpledb.parallel.ShuffleProducer;
import simpledb.parallel.SingleFieldHashPartitionFunction;
import simpledb.systemtest.ParallelTestBase;

public class ParallelAggregateTest extends ParallelTestBase {

    private void testAggregate(String agg,Aggregator.Op upOp, Aggregator.Op downOp) throws Exception
    {
        AggregateOptimizer ao = new AggregateOptimizer();
        TransactionId tid = new TransactionId();
        Class<? extends PartitionFunction<?, ?>> partitionFunction = SingleFieldHashPartitionFunction.class;
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        Parser p = new Parser();
        DbIterator noGroup = p.generateLogicalPlan(tid,
                "select "+agg+"(id) from student;").physicalPlan(tid,
                TableStats.getStatsMap(), false);
        Aggregate noGroupAgg = (Aggregate)(((Project)noGroup).getChildren()[0]);

        ParallelQueryPlan noGroupPlan = ParallelQueryPlan.parallelizeQueryPlan(
                tid, noGroup, workers, workers[0], server, partitionFunction);
        noGroupPlan = ao.optimize(tid, noGroupPlan);
        CollectProducer master = noGroupPlan.getMasterWorkerPlan();
        Project prj = (Project) master.getChildren()[0];
        Assert.assertTrue(prj.getChildren()[0] instanceof Rename);
        Rename re= (Rename)prj.getChildren()[0];
        Assert.assertTrue(re.getChildren()[0] instanceof Aggregate);
        Aggregate upAgg = (Aggregate) re.getChildren()[0];
        Assert.assertTrue(upAgg.getChildren()[0] instanceof CollectConsumer);
        Assert.assertTrue(((CollectConsumer) upAgg.getChildren()[0])
                .getChildren()[0] instanceof CollectProducer);
        
        Aggregate downAgg = (Aggregate) (((CollectProducer) (((CollectConsumer) (upAgg
                .getChildren()[0])).getChildren()[0])).getChildren()[0]);
        
        Assert.assertTrue(upAgg.groupField() == Aggregator.NO_GROUPING);
        Assert.assertTrue(upAgg.aggregateOp() == upOp);
        Assert.assertTrue(upAgg.aggregateField() == 0);

        Assert.assertTrue(downAgg.groupField() == Aggregator.NO_GROUPING);
        Assert.assertTrue(downAgg.aggregateOp() == downOp);
        Assert.assertTrue(downAgg.aggregateField() == noGroupAgg.aggregateField());
        Assert.assertTrue(re.getTupleDesc().getFieldName(0).equals(noGroupAgg.aggregateFieldName()));

        DbIterator group = p.generateLogicalPlan(tid,
                "select name,"+agg+"(id) from student group by name;")
                .physicalPlan(tid, TableStats.getStatsMap(), false);
        Aggregate groupAgg = (Aggregate)(((Project)group).getChildren()[0]);
        
        ParallelQueryPlan groupPlan = ParallelQueryPlan.parallelizeQueryPlan(
                tid, group, workers, workers[0], server, partitionFunction);
        groupPlan = ao.optimize(tid, groupPlan);
        master = groupPlan.getMasterWorkerPlan();
        re= (Rename)(((Project)(master.getChildren()[0])).getChildren()[0]);
        upAgg = (Aggregate)re.getChildren()[0];
        
        Assert.assertTrue(upAgg.getChildren()[0] instanceof ShuffleConsumer);
        ShuffleConsumer sc = (ShuffleConsumer)(upAgg.getChildren()[0]);
        Assert.assertTrue(sc.getChildren()[0] instanceof ShuffleProducer);
        ShuffleProducer sp = (ShuffleProducer)(sc.getChildren()[0]);
        Assert.assertTrue(sp.getChildren()[0] instanceof Aggregate);
        downAgg = (Aggregate)sp.getChildren()[0];
        
        Assert.assertTrue(upAgg.groupField() == 0);
        Assert.assertTrue(upAgg.aggregateOp() == upOp);
        Assert.assertTrue(upAgg.aggregateField() == 1);

        Assert.assertTrue(downAgg.groupField() == groupAgg.groupField());
        Assert.assertTrue(downAgg.aggregateOp() == downOp);
        Assert.assertTrue(downAgg.aggregateField() == groupAgg.aggregateField());
        Assert.assertTrue(re.getTupleDesc().getFieldName(1).equals(groupAgg.aggregateFieldName()));
        Assert.assertTrue(re.getTupleDesc().getFieldName(0).equals(groupAgg.groupFieldName()));
    }
    
    @Test
    public void parallelCountTest() throws Throwable {
        testAggregate("count",Aggregator.Op.SUM,Aggregator.Op.COUNT);
    }

    @Test
    public void parallelSumTest() throws Throwable {
        testAggregate("sum",Aggregator.Op.SUM,Aggregator.Op.SUM);
    }

    @Test
    public void parallelMinTest() throws Throwable {
        testAggregate("min",Aggregator.Op.MIN,Aggregator.Op.MIN);
    }

    @Test
    public void parallelAvgTest() throws Throwable {
        testAggregate("avg",Aggregator.Op.SC_AVG,Aggregator.Op.SUM_COUNT);
    }
}