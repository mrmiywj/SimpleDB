package simpledb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.junit.Test;

import simpledb.parallel.CollectProducer;
import simpledb.parallel.Consumer;
import simpledb.parallel.ExchangeMessage;
import simpledb.parallel.ParallelQueryPlan;
import simpledb.parallel.ParallelUtility;
import simpledb.parallel.PartitionFunction;
import simpledb.parallel.Producer;
import simpledb.parallel.SingleFieldHashPartitionFunction;
import simpledb.parallel.TupleBag;
import simpledb.parallel.Worker;
import simpledb.parallel.SocketInfo;
import simpledb.parallel.Exchange.ParallelOperatorID;
import simpledb.systemtest.ParallelTestBase;

public class WorkerTest extends ParallelTestBase {

    @SuppressWarnings("unchecked")
    public static <P extends DbIterator> P serializePlan(P plan)
            throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(plan);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                baos.toByteArray()));
        return (P) ois.readObject();
    }

    public void checkExchangeOperatorLocalization(DbIterator plan, Worker worker)
            throws Exception {
        if (plan == null)
            return;

        if (plan instanceof Producer) {
            Assert.assertEquals(worker, ((Producer) plan).getThisWorker());
        } else if (plan instanceof Consumer) {
            ParallelOperatorID oid = ((Consumer) plan).getOperatorID();
            TupleBag tb = new TupleBag(oid, workers[0].getId());
            worker.inBuffer.get(oid).put(tb);
            Assert.assertEquals(tb, ((Consumer) plan).take(5000));
        }

        DbIterator[] children = null;
        if (plan instanceof Operator)
            children = ((Operator) plan).getChildren();

        if (children != null)
            for (DbIterator child : children)
                if (child != null)
                    checkExchangeOperatorLocalization(child, worker);
    }

    @Test
    public void queryLocalizationTest() throws Exception {
        TransactionId tid = new TransactionId();
        Class<? extends PartitionFunction<?, ?>> partitionFunction = SingleFieldHashPartitionFunction.class;
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        Parser p = new Parser();
        DbIterator plan = p
                .generateLogicalPlan(
                        tid,
                        "select student.name,advisor.name from student,advisor where student.aid=advisor.id;")
                .physicalPlan(tid, TableStats.getStatsMap(), false);
        DbIterator serializedPlan = serializePlan(plan);

        ParallelQueryPlan parallelPlan = ParallelQueryPlan
                .parallelizeQueryPlan(tid, plan, workers, workers[0], server,
                        partitionFunction);

        CollectProducer masterPlan = parallelPlan.getMasterWorkerPlan();
        CollectProducer slavePlan = parallelPlan.getSlaveWorkerPlan();

        CollectProducer serializedMasterPlan = serializePlan(masterPlan);
        CollectProducer serializedSlavePlan = serializePlan(slavePlan);

     // create a new test base
        ParallelTestBase newTestBase = new ParallelTestBase();
        newTestBase.init();
        
        try {
            Database.getCatalog().loadSchema(
                    newTestBase.schemaFile.getAbsolutePath());
            TableStats.computeStatistics();

            try {
                Query q = new Query(tid);
                q.setPhysicalPlan(serializedPlan);
                /**
                 * The query should throw an exception because the database has
                 * completely changed, the tables in the serialized plan do not
                 * exist (tables are identified by their ID, not their name)
                 * */
                q.execute();
            } catch (Exception e) {
                Worker worker = new Worker(workers[0].getId(), server.getId());

                worker.localizeQueryPlan(serializedPlan);

                Query q = new Query(tid);
                q.setPhysicalPlan(serializedPlan);
                /**
                 * now the query should work without problems, the
                 * localizeQueryPlan method should reset the old table id with
                 * the new id of the table with the same table name.
                 */
                q.execute();

                ArrayList<ParallelOperatorID> oIds = new ArrayList<ParallelOperatorID>();
                Worker.collectConsumerOperatorIDs(serializedMasterPlan, oIds);
                for (ParallelOperatorID id : oIds) {
                    worker.inBuffer.put(id,
                            new LinkedBlockingQueue<ExchangeMessage>());
                }

                worker.localizeQueryPlan(serializedMasterPlan);
                checkExchangeOperatorLocalization(serializedMasterPlan, worker);
                worker.localizeQueryPlan(serializedSlavePlan);
                checkExchangeOperatorLocalization(serializedSlavePlan, worker);
                return;
            }
            throw new RuntimeException(
                    "Error, should not reach here. The database has completely changed all its tables although the names are still the same.");
        } finally {
            newTestBase.clean();
        }
    }

    @Test
    public void workerQueryExecutionTest() throws Exception {
        System.out.println("in worker query execution test");
        TransactionId tid = new TransactionId();
        Class<? extends PartitionFunction<?, ?>> partitionFunction = SingleFieldHashPartitionFunction.class;
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        Parser p = new Parser();
        DbIterator localPlan = p.generateLogicalPlan(tid,
                "select student.name from student where student.aid>=0;")
                .physicalPlan(tid, TableStats.getStatsMap(), false);

        localPlan.open();
        HashSet<String> tuples = new HashSet<String>();

        while (localPlan.hasNext()) {
            Tuple t = localPlan.next();
            tuples.add(t.toString());
        }
        localPlan.close();

        ParallelQueryPlan parallelPlan = ParallelQueryPlan
                .parallelizeQueryPlan(tid, localPlan,
                        new SocketInfo[] { workers[0] }, workers[0], server,
                        partitionFunction);

        final CollectProducer masterPlan = parallelPlan.getMasterWorkerPlan();

        final ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<ExchangeMessage>> serverBuffer = new ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<ExchangeMessage>>();
        NioSocketAcceptor pseudoServer = ParallelUtility.createAcceptor();
        final Worker worker = new Worker(workers[0].getId(), server.getId());
        try {
            pseudoServer.setHandler(new IoHandlerAdapter() {
                public void messageReceived(IoSession session, Object message) {
                    if (message instanceof TupleBag) {
                        TupleBag tuples = (TupleBag) message;

                        LinkedBlockingQueue<ExchangeMessage> q = serverBuffer
                                .get(tuples.getOperatorID());
                        if (q == null) {
                            q = new LinkedBlockingQueue<ExchangeMessage>();
                            serverBuffer.put(tuples.getOperatorID(), q);
                        }
                        q.offer(tuples);
                    }
                }
            });
            pseudoServer.bind(this.server.getAddress());

            worker.init();
            worker.start();
            worker.receiveQuery(serializePlan(masterPlan));
            worker.executeQuery();
            long timeoutMS = 5 * 1000;
            long current = System.currentTimeMillis();
            while (worker.isRunning()) {
                Thread.sleep(100);
                if (System.currentTimeMillis() - current > timeoutMS)
                    break;
            }
        } finally {
            worker.shutdown();
            System.out.println("Before unbind");
            ParallelUtility.unbind(pseudoServer);
            System.out.println("After unbind");
        }

        // The worker should have finished the query;
        Assert.assertTrue(!worker.isRunning());

        HashSet<String> serverReceivedTuples = new HashSet<String>();
        boolean seenEOS = false;
        long start = System.currentTimeMillis();
        getData: while (true) {
            for (ParallelOperatorID oid : serverBuffer.keySet()) {
                LinkedBlockingQueue<ExchangeMessage> q = serverBuffer.get(oid);
                TupleBag tb = null;
                while ((tb = (TupleBag) q.poll()) != null) {
                    if (tb.isEos()) {
                        seenEOS = true;
                        break getData;
                    }
                    Iterator<Tuple> it = tb.iterator();
                    while (it.hasNext()) {
                        serverReceivedTuples.add(it.next().toString());
                    }
                }
            }
            long end = System.currentTimeMillis();
            if (end - start > 10 * 1000)
                // Receive data for at most ten seconds, if time expire, exit
                // directly
                break getData;
            if (!seenEOS)
                Thread.sleep(10);

        }

        // EOS message should have been received
        Assert.assertTrue(seenEOS);

        // The server's received tuples should be the same as the output of the
        // local
        // query.
        Assert.assertTrue(serverReceivedTuples.containsAll(tuples)
                && tuples.containsAll(serverReceivedTuples));

        System.out.println("FInal");
    }

}
