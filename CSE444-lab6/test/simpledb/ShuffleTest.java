package simpledb;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.junit.Test;

import simpledb.parallel.ExchangeMessage;
import simpledb.parallel.ParallelUtility;
import simpledb.parallel.PartitionFunction;
import simpledb.parallel.ShuffleConsumer;
import simpledb.parallel.ShuffleProducer;
import simpledb.parallel.TupleBag;
import simpledb.parallel.Worker;
import simpledb.parallel.Exchange.ParallelOperatorID;
import simpledb.systemtest.ParallelTestBase;

public class ShuffleTest extends ParallelTestBase {

    static class IdParityPartitionFunction extends
            PartitionFunction<String, String> {

        public IdParityPartitionFunction(int numPartition) {
            super(numPartition);
        }

        private static final long serialVersionUID = 1L;

        @Override
        public int partition(Tuple t, TupleDesc td) {
            IntField i = (IntField) (t.getField(td.fieldNameToIndex("s.id")));
            if (i.getValue() % 2 == 0)
                return 0;
            else
                return 1;
        }
    }

    static class MockHandler extends IoHandlerAdapter {
        BitSet eos = null;
        Thread mainThread = null;
        int id;
        HashSet<String> tuples = null;

        public MockHandler(BitSet eos, Thread mainThread, int id,
                HashSet<String> tuples) {
            this.eos = eos;
            this.mainThread = mainThread;
            this.id = id;
            this.tuples = tuples;
        }

        @Override
        public void messageReceived(IoSession session, Object message) {
            if (!(message instanceof TupleBag)) {
                mainThread.interrupt();
            } else {
                TupleBag tb = ((TupleBag) message);
                if (tb.isEos()) {
                    eos.set(id);
                } else {
                    Iterator<Tuple> tuples = tb.iterator();
                    while (tuples.hasNext()) {
                        Tuple t = tuples.next();
                        this.tuples.add(t.toString());
                    }
                }
            }
        }
    };

    @SuppressWarnings("unchecked")
    @Test
    public void shuffleProducerTest() throws Exception {
        TransactionId tid = new TransactionId();
        IdParityPartitionFunction partitionFunction = new IdParityPartitionFunction(
                2);
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        SeqScan scan = new SeqScan(tid, Database.getCatalog().getTableId(
                "student"), "s");
        HashSet<String>[] workerExpectedTuples = new HashSet[2];
        workerExpectedTuples[0] = new HashSet<String>();
        workerExpectedTuples[1] = new HashSet<String>();

        scan.open();
        TupleDesc td = scan.getTupleDesc();
        while (scan.hasNext()) {
            Tuple t = scan.next();
            workerExpectedTuples[partitionFunction.partition(t, td)].add(t
                    .toString());
        }

        scan.close();

        final BitSet eos = new BitSet();
        final Thread mainThread = Thread.currentThread();
        final HashSet<String>[] workerTuples = new HashSet[2];
        workerTuples[0] = new HashSet<String>();
        workerTuples[1] = new HashSet<String>();

        ShuffleProducer sp = new ShuffleProducer(scan,
                ParallelOperatorID.newID(tid), workers, partitionFunction);
        sp.setThisWorker(new Worker(workers[0].getId(), server.getId()));

        NioSocketAcceptor worker1 = ParallelUtility.createAcceptor();
        NioSocketAcceptor worker2 = ParallelUtility.createAcceptor();
        try {
            worker1.setHandler(new MockHandler(eos, mainThread, 0,
                    workerTuples[0]));
            worker1.bind(workers[0].getAddress());

            worker2.setHandler(new MockHandler(eos, mainThread, 1,
                    workerTuples[1]));
            worker2.bind(workers[1].getAddress());

            sp.open();

            // wait for 5 seconds.
            long timeoutMS = 5 * 1000;
            long current = System.currentTimeMillis();
            synchronized (mainThread) {
                while (eos.nextClearBit(0) <= 1)
                    try {
                        Thread.sleep(10);
                        if (System.currentTimeMillis() - current > timeoutMS)
                            break;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                "messages of non TupleBag types received");
                    }
            }
        } finally {

            ParallelUtility.unbind(worker1);
            ParallelUtility.unbind(worker2);
        }
        // Both the workers should have received the EOS
        Assert.assertTrue(eos.nextClearBit(0) > 1);

        // The tuples received by worker0 should be the same as expected, i.e.
        // the tuples with even id
        Assert.assertTrue(workerExpectedTuples[0].containsAll(workerTuples[0])
                && workerTuples[0].containsAll(workerExpectedTuples[0]));

        // The tuples received by worker1 should be the same as expected, i.e.
        // the tuples with odd id
        Assert.assertTrue(workerExpectedTuples[1].containsAll(workerTuples[1])
                && workerTuples[1].containsAll(workerExpectedTuples[1]));
    }

    @Test
    public void shuffleConsumerTest() throws Exception {
        TransactionId tId = new TransactionId();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        SeqScan scan = new SeqScan(tId, Database.getCatalog().getTableId(
                "student"), "s");

        ParallelOperatorID oid = ParallelOperatorID.newID(tId);

        LinkedBlockingQueue<ExchangeMessage> mockBuffer = new LinkedBlockingQueue<ExchangeMessage>();

        scan.open();
        TupleBag eos0 = new TupleBag(oid, workers[0].getId());
        TupleBag eos1 = new TupleBag(oid, workers[1].getId());
        mockBuffer.add(eos1);
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        ArrayList<TupleBag> tupleBagsWithoutEOS1 = new ArrayList<TupleBag>();

        HashSet<Tuple> allTuples = new HashSet<Tuple>();

        while (scan.hasNext()) {
            Tuple t = scan.next();
            tuples.add(t);
            allTuples.add(t);
            if (tuples.size() >= TupleBag.MIN_SIZE) {
                TupleBag tb = new TupleBag(oid, workers[0].getId(),
                        tuples.toArray(new Tuple[] {}), scan.getTupleDesc());
                tupleBagsWithoutEOS1.add(tb);
                mockBuffer.add(tb);
                tuples.clear();
            }
        }
        tupleBagsWithoutEOS1.add(eos0);

        mockBuffer.add(new TupleBag(oid, workers[0].getId(), tuples
                .toArray(new Tuple[] {}), scan.getTupleDesc()));
        mockBuffer.add(eos0);

        scan.close();

        ShuffleConsumer sc = new ShuffleConsumer(null, oid, workers);
        sc.setBuffer(mockBuffer);
        sc.open();
        HashSet<Tuple> receivedTuples = new HashSet<Tuple>();
        while (sc.hasNext()) {
            receivedTuples.add(sc.next());
        }
        sc.close();

        mockBuffer.clear();
        Assert.assertTrue(receivedTuples.containsAll(allTuples)
                && allTuples.containsAll(receivedTuples));

        for (int i = 0; i < tupleBagsWithoutEOS1.size(); i++)
            mockBuffer.add(tupleBagsWithoutEOS1.get(i));

        final Thread mainThread = Thread.currentThread();
        new Thread() {
            public void run() {
                // wait for ten seconds;
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                mainThread.interrupt();
            }
        }.start();

        sc = new ShuffleConsumer(null, oid, workers);
        sc.setBuffer(mockBuffer);
        sc.open();
        try {
            while (sc.hasNext()) {
                sc.next();
            }
        } catch (DbException e) {
            sc.close();
            return;
        }

        throw new RuntimeException(
                "The Shuffle Consumer should wait endlessly for the EOS from worker1, should not reach here!");

    }
}