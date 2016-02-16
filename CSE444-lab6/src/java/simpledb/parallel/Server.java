package simpledb.parallel;

import java.awt.GraphicsEnvironment;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.util.ConcurrentHashSet;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.Operator;
import simpledb.OperatorCardinality;
import simpledb.Parser;
import simpledb.ParsingException;
import simpledb.Query;
import simpledb.QueryPlanVisualizer;
import simpledb.TableStats;
import simpledb.Transaction;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.parallel.Exchange.ParallelOperatorID;

import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

/**
 * The parallel SimpleDB is consisted of one server and a bunch of workers.
 * 
 * The server accepts SQL from commandline, generate a query plan, optimize it,
 * parallelize it. And then the server send a query plan to each worker. Each
 * worker may receive different query plans. The server does not execute any
 * part of a query. The server then receives query results from the workers and
 * display them in the commandline.
 * 
 * The data of a database is averagely spread over all the workers. The
 * execution of each query is conducted simultaneously at each worker.
 * 
 * Currently, we only support equi-join.
 * 
 * The server and the workers run in separate processes. They may or may not
 * cross the boundary of computers.
 * 
 * The server address is configurable at conf/server.conf, the default is
 * 
 * localhost:24444
 * 
 * The worker addresses are configurable at conf/workers.conf, one line for each
 * worker. the default are
 * 
 * localhost:24448 localhost:24449
 * 
 * You may add more workers by adding more host:port lines.
 * 
 * You may change the configurations to cross computers. But make that, the
 * server is able to access the workers and each worker is able to access both
 * the server and all other workers.
 * 
 * 
 * Given a query, there can be up to two different query plans. If the query
 * contains order by or aggregates without group by, a "master worker" is needed
 * to collect the tuples to be ordered or to be aggregated from all the workers.
 * All the other workers are correspondingly called slave workers.
 * 
 * For example, for a query:
 * 
 * select * from actor order by id;
 * 
 * at slave workers, the query plan is:
 * 
 * scan actor -> send data to the master worker
 * 
 * at the master worker, the query plan is:
 * 
 * scan actor -> send data to myself -> collect data from all the workers ->
 * order by -> send output to the server
 * 
 * 
 * at slave workers: | at the master worker: | at the server: | | worker_1 :
 * scan actor | | worker_2 : scan actor | | ... | collect -> order by (id)
 * ------| ---> output result worker_(n-1): scan actor | | worker_n : scan actor
 * | |
 * 
 * If there is no order by and aggregate without group by, all the workers will
 * have the same query plan. The results of each worker are sent directly to the
 * server.
 * 
 * 
 * We use Apache Mina for inter-process communication. Mina is a wrapper library
 * of java nio. To start using Mina, http://mina.apache.org/user-guide.html is a
 * good place to seek information.
 * 
 * 
 * 
 * */
public class Server extends Parser {

    static final String usage = "Usage: Server catalogFile [--conf confdir] [-explain] [-f queryFile]";

    final SocketInfo[] workers;
    final HashMap<String, Integer> workerIdToIndex;
    final NioSocketAcceptor acceptor;
    final ServerHandler minaHandler;

    /**
     * The I/O buffer, all the ExchangeMessages sent to the server are buffered
     * here.
     * */
    final ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<ExchangeMessage>> inBuffer;
    final SocketInfo server;

    /**
     * check if currently the system is under gui mode. Used only to control the
     * output of the results
     */
    static boolean isGUI;

    public static final String DEFAULT_CONF_DIR = "conf";

    public static SocketInfo loadServer(String confDir) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(confDir + "/server.conf"))));
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "")
                    .split(":");
            if (ts.length == 2)
                return new SocketInfo(ts[0], Integer.parseInt(ts[1]));
        }
        throw new IOException("Wrong server conf file.");
    }

    protected Server(String host, int port, SocketInfo[] workers)
            throws IOException {
        this.workers = workers;
        workerIdToIndex = new HashMap<String, Integer>();
        for (int i = 0; i < workers.length; i++)
            workerIdToIndex.put(workers[i].getId(), i);
        acceptor = ParallelUtility.createAcceptor();
        this.server = new SocketInfo(host, port);
        this.inBuffer = new ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<ExchangeMessage>>();
        this.minaHandler = new ServerHandler(Thread.currentThread());
    }

    protected void init() throws IOException {
        // Bind

        acceptor.setHandler(this.minaHandler);
        acceptor.bind(this.server.getAddress());
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 6) {
            System.err.println("Invalid number of arguments.\n" + usage);
            System.exit(0);
        }

        String confDir = DEFAULT_CONF_DIR;
        if (args.length >= 3 && args[1].equals("--conf")) {
            confDir = args[2];
            args = ParallelUtility.removeArg(args, 1);
            args = ParallelUtility.removeArg(args, 1);
        }

        if (GraphicsEnvironment.isHeadless()) {
            // text console mode
            isGUI = false;
        } else {
            // gui mode
            isGUI = true;
        }

        SocketInfo serverInfo = loadServer(confDir);
        final Server server = new Server(serverInfo.getHost(),
                serverInfo.getPort(), ParallelUtility.loadWorkers(confDir));

        System.out.println("Workers are: ");
        for (SocketInfo w : server.workers)
            System.out.println("  " + w.getHost() + ":" + w.getPort());

        server.init();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                server.cleanup();
            }
        });
        System.out.println("Server: " + server.server.getHost()
                + " started. Listening on port " + serverInfo.getPort());
        server.start(args);

    }

    public void shutdown() {
        ParallelUtility.shutdownVM();
    }

    public void cleanup() {
        System.out.println("SimpleDB is going to shutdown");
        System.out
                .println("Send shutdown requests to the workers, please wait");
        for (SocketInfo worker : workers) {
            System.out.println("Shuting down " + worker.getId());
            // IoConnector connector = ParallelUtility.createConnector();
            // connector.setHandler(new IoHandlerAdapter());
            // connector.setConnectTimeoutMillis(3000000);
            // ConnectFuture future = connector.connect(worker.getAddress());
            // future.awaitUninterruptibly();

            IoSession session;
            try {
                session = ParallelUtility.createSession(worker.getAddress(),
                        this.minaHandler, 3000);
            } catch (Throwable e) {
//                e.printStackTrace();
                session = null;
            }
            if (session == null) {
                System.out.println("Fail to connect the worker "+worker+". continue cleaning");
                continue;
            }
            // IoSession session = future.getSession();
            session.write("shutdown").addListener(
                    new IoFutureListener<WriteFuture>() {

                        @Override
                        public void operationComplete(WriteFuture future) {
                            ParallelUtility.closeSession(future.getSession());
                        }
                    });
            System.out.println("Done");
        }
        ParallelUtility.unbind(acceptor);
        System.out.println("Bye");
    }

    protected class ServerHandler extends IoHandlerAdapter {

        // required in ParallelTest for instrument
        final Thread mainThread;

        public ServerHandler(Thread mainThread) {
            this.mainThread = mainThread;
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            cause.printStackTrace();
            ParallelUtility.closeSession(session);
        }

        @Override
        public void messageReceived(IoSession session, Object message) {
            if (message instanceof TupleBag) {
                TupleBag tuples = (TupleBag) message;
                Server.this.inBuffer.get(tuples.getOperatorID()).offer(tuples);
            } else if (message instanceof String) {
                System.out.println("Query received by worker");
                Integer workerId = Server.this.workerIdToIndex.get(message);
                if (workerId != null) {
                    Server.this.queryReceivedByWorker(workerId);
                }
            }

        }

        public void sessionIdle(IoSession session, IdleStatus status) {
            if (status.equals(IdleStatus.BOTH_IDLE)) {
                session.close(false);
            }
        }
    }

    protected void queryReceivedByWorker(int workerId) {
        workersReceivedQuery.add(workerId);
        if (workersReceivedQuery.size() >= this.workers.length) {
            ParallelUtility.broadcastMessageToWorkers("start", workers,
                    this.minaHandler, -1);
            this.workersReceivedQuery.clear();
        }
    }

    final ConcurrentHashSet<Integer> workersReceivedQuery = new ConcurrentHashSet<Integer>();

    /**
     * Select the master worker for the coming query
     * */
    protected SocketInfo selectMasterWorker() {
        int master = (int) (Math.random() * this.workers.length);
        return this.workers[master];
    }

    public SocketInfo[] getWorkers() {
        return this.workers;
    }

    protected void dispatchWorkerQueryPlans(
            HashMap<SocketInfo, DbIterator> plans) {
        for (Map.Entry<SocketInfo, DbIterator> e : plans.entrySet()) {
            SocketInfo worker = e.getKey();
            DbIterator plan = e.getValue();
            IoSession ssss0 = ParallelUtility.createSession(
                    worker.getAddress(), this.minaHandler, -1);
            // this session will be reused for the Workers to report the receive
            // of the queryplan, therefore, do not close it
            ssss0.write(plan);
        }

    }

    protected void startQuery(ParallelQueryPlan queryPlan,
            SocketInfo masterWorker) throws DbException,
            TransactionAbortedException {
        HashMap<SocketInfo, DbIterator> workerPlans = new HashMap<SocketInfo, DbIterator>();
        for (SocketInfo worker : this.workers) {
            if (worker == masterWorker) {
                workerPlans.put(worker, queryPlan.getMasterWorkerPlan());
            } else
                workerPlans.put(worker, queryPlan.getSlaveWorkerPlan());
        }

        CollectConsumer serverPlan = queryPlan.getServerPlan();
        final LinkedBlockingQueue<ExchangeMessage> buffer = new LinkedBlockingQueue<ExchangeMessage>();
        serverPlan.setBuffer(buffer);
        Server.this.inBuffer.put(serverPlan.getOperatorID(), buffer);

        dispatchWorkerQueryPlans(workerPlans);

        TupleDesc td = serverPlan.getTupleDesc();

        String names = "";
        for (int i = 0; i < td.numFields(); i++) {
            names += td.getFieldName(i) + "\t";
        }

        PrintStream out = null;
        ByteArrayOutputStream b = null;
        if (isGUI)
            out = System.out;
        else
            // If output is not X, buffer the output
            out = new PrintStream(b = new java.io.ByteArrayOutputStream());

        out.println(names);
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            out.print("-");
        }
        out.println("");

        serverPlan.open();
        int cnt = 0;
        while (serverPlan.hasNext()) {
            Tuple tup = serverPlan.next();
            out.println(tup);
            cnt++;
        }
        out.println("\n " + cnt + " rows.");
        if (b != null)
            System.out.print(b.toString());

        serverPlan.close();
        Server.this.inBuffer.remove(serverPlan.getOperatorID());
    }

    protected void processQuery(Query q) throws DbException,
            TransactionAbortedException {
        HashMap<String, Integer> aliasToId = q.getLogicalPlan()
                .getTableAliasToIdMapping();
        DbIterator sequentialQueryPlan = q.getPhysicalPlan();
        Map<String, TableStats> tableStats = TableStats.getStatsMap();
        TransactionId tid = q.getTransactionId();

        if (sequentialQueryPlan != null) {
            System.out.println("The sequential query plan is:");
            OperatorCardinality.updateOperatorCardinality(
                    (Operator) sequentialQueryPlan, aliasToId, tableStats);
            new QueryPlanVisualizer().printQueryPlanTree(sequentialQueryPlan,
                    System.out);
        }

        SocketInfo masterWorker = selectMasterWorker();

        ParallelQueryPlan p = ParallelQueryPlan.parallelizeQueryPlan(tid,
                sequentialQueryPlan, workers, masterWorker, server,
                SingleFieldHashPartitionFunction.class);

        p = ParallelQueryPlan.optimize(tid, p, new ProjectOptimizer(
                new FilterOptimizer(
                        new AggregateOptimizer(new BloomFilterOptimizer(
                                workers, aliasToId, tableStats)))));

        // p = ParallelQueryPlan.optimize(tid, p, new FilterOptimizer());
        //
        // p = ParallelQueryPlan.optimize(tid, p, new AggregateOptimizer());
        //
        // p = ParallelQueryPlan.optimize(tid, p, new BloomFilterOptimizer(
        // workers, aliasToId, tableStats));

        System.out.println("Master worker plan: ");
        new QueryPlanVisualizer().printQueryPlanTree(p.getMasterWorkerPlan(),
                System.out);

        System.out.println("Slave worker plan: ");
        new QueryPlanVisualizer().printQueryPlanTree(p.getSlaveWorkerPlan(),
                System.out);

        System.out.println("Server plan: ");
        new QueryPlanVisualizer().printQueryPlanTree(p.getServerPlan(),
                System.out);

        startQuery(p, masterWorker);
    }

    public void processNextStatement(InputStream is) {
        try {
            Transaction t = new Transaction();
            setTransaction(t);
            t.start();
            super.setTransaction(t);
            try {
                ZqlParser p = new ZqlParser(is);
                ZStatement s = p.readStatement();
                Query q = null;

                if (s instanceof ZQuery)
                    q = handleQueryStatement((ZQuery) s, t.getId());
                else {
                    System.err
                            .println("Currently only query statements (select) are supported");
                    return;
                }
                processQuery(q);
                t.commit();
                t = null;
            } catch (Throwable a) {
                // Whenever error happens, abort the current transaction

                if (t != null) {
                    t.abort();
                    System.out.println("Transaction " + t.getId().getId()
                            + " aborted because of unhandled error");
                }

                if (a instanceof simpledb.ParsingException
                        || a instanceof Zql.ParseException)
                    throw new ParsingException((Exception) a);
                if (a instanceof Zql.TokenMgrError)
                    throw (Zql.TokenMgrError) a;
                throw new DbException(a.getMessage());
            } finally {
                t = null;
                super.setTransaction(null);
            }

        } catch (DbException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (simpledb.ParsingException e) {
            System.out
                    .println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (Zql.TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);

        }
    }
}
