package simpledb.parallel;

import simpledb.TransactionId;

/**
 * Parallel query plan optimizer super class. All the parallel query plan optimizers
 * should be inherited from this class.
 * 
 * Given input a parallel query plan, the output is the optimized parallel query plan
 * 
 * Note that the input query plan and the output query should both valid, i.e.
 * can be directly used for running. 
 * 
 * This is required so that we can chain a set of optimizers.
 * 
 * */
public abstract class ParallelQueryPlanOptimizer {
    ParallelQueryPlanOptimizer next = null;

    public ParallelQueryPlanOptimizer()
    {
        this.next = null;
    }
    
    /**
     * @param next
     *       The next optimizer. The output of this optimizer will be
     *       fed as the input to the next optimizer.
     * */
    public ParallelQueryPlanOptimizer(ParallelQueryPlanOptimizer next) {
        this.next = next;
    }

    /**
     * Do the optimization
     * @param tid
     *      TransactionId of the query
     * @param plan
     *      The plan to be optimized
     * @param optimizedBy
     *      Keep the record of what optimizations have been conducted on
     *      the query
     *     
     * */
    final public ParallelQueryPlan optimize(TransactionId tid, ParallelQueryPlan plan) {
        if (! plan.optimizedBy.contains(this.getClass()))
        {
            this.doMyOptimization(tid,plan);
            plan.optimizedBy.add(this.getClass());
        }

        if (this.next != null)
            return this.next.optimize(tid,plan);
        return plan;
    }

    /**
     * Sub classes should implement this method to do the actual optimization
     * */
    abstract protected void doMyOptimization(TransactionId tid, ParallelQueryPlan plan);
}
