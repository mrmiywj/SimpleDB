package simpledb.systemtest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javassist.ClassPool;
import javassist.CtClass;

import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.util.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Test;

import simpledb.Database;
import simpledb.DbIterator;
import simpledb.TableStats;
import simpledb.TransactionId;
import simpledb.parallel.ParallelUtility;

public class ParallelTest extends ParallelTestBase {

    public static final ConcurrentHashSet<NioSocketAcceptor> sockets = new ConcurrentHashSet<NioSocketAcceptor>();

     @Test
    public void parallelCount() throws Throwable {
        System.out.println("Start test count");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select count(id) from student;";

        File queryFile = new File(testDir.getAbsolutePath()
                + "/query_count.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

     @Test
    public void parallelMax() throws Throwable {
        System.out.println("Start test max");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select max(id) from student;";

        File queryFile = new File(testDir.getAbsolutePath() + "/query_max.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

     @Test
    public void parallelMin() throws Throwable {
        System.out.println("Start test min");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select min(id) from student;";

        File queryFile = new File(testDir.getAbsolutePath() + "/query_min.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

     @Test
    public void parallelAvg() throws Throwable {
        System.out.println("Start test avg");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select avg(id) from student;";

        File queryFile = new File(testDir.getAbsolutePath() + "/query_avg.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

     @Test
    public void parallelGroupBy() throws Throwable {
        System.out.println("Start test group by");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select name,count(id) from student group by name;";

        File queryFile = new File(testDir.getAbsolutePath()
                + "/query_groupby.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

    @Test
    public void parallelJoin() throws Throwable {
        System.out.println("Start test join");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select student.name,advisor.name from student,advisor where student.aid=advisor.id;";

        File queryFile = new File(testDir.getAbsolutePath() + "/query_join.sql");
        ParallelUtility.writeFile(queryFile, query);
        HashMap<String, Integer> expected = new HashMap<String, Integer>();
        int numTuples = getExpectedBagOfTuples(query, expected);

        try {
            startSimpleDB(queryFile, expected, null, numTuples);
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }
    }

     @Test
    public void parallelOrderBy() throws Throwable {
        System.out.println("Start test order by");
        System.out.println("Test directory is: " + testDir.getAbsolutePath());
        instrumentServer();
        instrumentWorker();
        sockets.clear();
        instrumentUtility();
        Database.getCatalog().loadSchema(this.schemaFile.getAbsolutePath());
        TableStats.computeStatistics();
        String query = "select * from advisor order by id desc;";

        File queryFile = new File(testDir.getAbsolutePath()
                + "/query_orderby.sql");
        ParallelUtility.writeFile(queryFile, query);

        Queue<String> expected = getExpectedListOfTuples(query);

        try {
            startSimpleDB(queryFile, null, expected, expected.size());
        } finally {
            for (NioSocketAcceptor a : sockets) {
                ParallelUtility.unbind(a);
            }
            sockets.clear();
        }

    }

    public Queue<String> getExpectedListOfTuples(String query) throws Exception {
        TransactionId tid = new TransactionId();

        DbIterator plan = new simpledb.Parser().generateLogicalPlan(tid, query)
                .physicalPlan(tid, TableStats.getStatsMap(), false);
        plan.open();
        Queue<String> results = new LinkedList<String>();
        while (plan.hasNext()) {
            String t = plan.next().toString();
            results.add(t);
        }
        plan.close();
        return results;
    }

    public int getExpectedBagOfTuples(String query,
            HashMap<String, Integer> results) throws Exception {
        TransactionId tid = new TransactionId();

        DbIterator plan = new simpledb.Parser().generateLogicalPlan(tid, query)
                .physicalPlan(tid, TableStats.getStatsMap(), false);
        plan.open();
        int numTuples = 0;
        while (plan.hasNext()) {
            String t = plan.next().toString();
            Integer existing = results.get(t);
            if (existing != null)
                results.put(t, existing + 1);
            else
                results.put(t, 1);
            numTuples++;
        }
        plan.close();
        return numTuples;
    }

    public void instrumentWorker() throws Exception {

        ClassPool cp = new ClassPool();
        cp.appendSystemPath();
        cp.insertClassPath(libDir.getAbsolutePath() + "/*");
        cp.insertClassPath(this.binDir.getAbsolutePath());

        // CtClass
        //
        // clas = cp.get("simpledb.parallel.Worker$WorkerLivenessController");
        // Assert.assertNotNull(clas);
        //
        // StringBuilder body = new StringBuilder();
        // body.append("return;\n");
        //
        // beforeMethodCall(clas, "run", body.toString());
        // clas.writeFile(binDir.getAbsolutePath());

    }

    public void instrumentServer() throws Exception {

        ClassPool cp = new ClassPool();
        cp.appendSystemPath();
        cp.insertClassPath(libDir.getAbsolutePath() + "/*");
        cp.insertClassPath(binDir.getAbsolutePath());

        CtClass clas = cp.get("simpledb.parallel.Server$ServerHandler");
        Assert.assertNotNull(clas);

        StringBuffer body2 = new StringBuffer();
        body2.append("$0.mainThread.interrupt();\n");

        afterMethodCall(clas, "exceptionCaught", body2.toString());

        StringBuilder body = new StringBuilder();
        body.append("simpledb.systemtest.ParallelTest.ServerTestThread thread= (simpledb.systemtest.ParallelTest.ServerTestThread)$0.mainThread;\n");
        body.append("if ($2 instanceof simpledb.parallel.TupleBag) {\n");
        body.append("    simpledb.parallel.TupleBag tb = (simpledb.parallel.TupleBag)$2;\n");
        body.append("    if (!tb.isEos()){\n");
        body.append("        java.util.Iterator it = tb.iterator();\n");
        body.append("        while (it.hasNext()){\n");
        body.append("            String ts=it.next().toString();\n");
        body.append("            if (thread.bagOfTuples.containsKey(ts)){\n");
        body.append("                Integer old = (Integer)thread.bagOfTuples.get(ts);\n");
        body.append("                thread.bagOfTuples.put(ts,new Integer(old.intValue()+1));\n");
        body.append("            }else{\n");
        body.append("                thread.bagOfTuples.put(ts,new Integer(1));\n");
        body.append("            }\n");
        body.append("            thread.orderedTuples.add(ts);\n");
        body.append("            thread.numTuples+=1;\n");
        body.append("        }\n");
        body.append("    }\n");
        body.append("}\n");

        beforeMethodCall(clas, "messageReceived", body.toString());
        clas.writeFile(binDir.getAbsolutePath());

        clas = cp.get("simpledb.parallel.Server");
        Assert.assertNotNull(clas);

        StringBuilder body3 = new StringBuilder();
        body3.append("$0.acceptor.unbind();\n");
        body3.append("$0.acceptor.dispose(true);\n");
        body3.append("while ($0.acceptor.isActive() || !$0.acceptor.isDisposed()){\n");
        body3.append("    try {\n");
        body3.append("        Thread.sleep(100l);\n");
        body3.append("    } catch (InterruptedException e) {}\n");
        body3.append("};\n");
        body3.append("System.out.println(\"Server is unbind\");\n");
        beforeMethodCall(clas, "shutdown", body3.toString());
        clas.writeFile(binDir.getAbsolutePath());
    }

    public void instrumentUtility() throws Exception {
        ClassPool cp = new ClassPool();
        cp.appendSystemPath();
        cp.insertClassPath(libDir.getAbsolutePath() + "/*");
        cp.insertClassPath(this.binDir.getAbsolutePath());

        CtClass clas = cp.get("simpledb.parallel.ParallelUtility");
        StringBuilder body3 = new StringBuilder();
        body3.append("simpledb.systemtest.ParallelTest.sockets.add(result);\n");
        afterMethodCall(clas, "createAcceptor", body3.toString());

        body3 = new StringBuilder();
        body3.append("return;\n");
        beforeMethodCall(clas, "shutdownVM", body3.toString());

        clas.writeFile(binDir.getAbsolutePath());
    }

    private class TestThread extends Thread {

        public TestThread(final File queryFile,
                final Map<String, Integer> expectedBagOfTuples,
                final Queue<String> expectedOrderdTuples, final int numTuples) {
            this.numTuples = numTuples;
            this.queryFile = queryFile;
            this.expectedBagOfTuples = expectedBagOfTuples;
            this.expectedOrderdTuples = expectedOrderdTuples;
        }

        public ArrayList<Throwable> exceptions = new ArrayList<Throwable>();
        File queryFile;
        final Map<String, Integer> expectedBagOfTuples;
        final Queue<String> expectedOrderdTuples;
        final int numTuples;

        public void run() {
            final Thread thisThread = this;
            Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    if (t != thisThread) {
                        exceptions.add(e);
                        thisThread.interrupt();
                    }
                }
            });
            startWorkers();
            java.io.InputStream is = new java.io.ByteArrayInputStream(
                    "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n".getBytes());
            System.setIn(is);
            try {
                ServerTestThread serverT = startServer(queryFile);
                long current = System.currentTimeMillis();
                long timeoutMS = 5 * 1000;
                while (// still working
                ((serverT.isAlive() && serverT.numTuples > 0)) ||
                // may be not working properly but let's wait for timoutMS
                        ((System.currentTimeMillis() - current <= timeoutMS) && serverT
                                .isAlive())) {
                    Thread.sleep(10);
                }
                // Server should at least has some result(numTuples>0)
                // within 10
                // seconds.
                Assert.assertTrue(!serverT.isAlive());

                Assert.assertEquals(numTuples, serverT.numTuples);
                if (expectedBagOfTuples != null)
                    assertBagEqual(expectedBagOfTuples, serverT.bagOfTuples);
                if (expectedOrderdTuples != null)
                    assertListEqual(expectedOrderdTuples,
                            (Queue<String>) serverT.orderedTuples);

            } catch (InterruptedException e) {
                if (exceptions.size() == 0)
                    exceptions.add(e);
                for (int i = 0; i < exceptions.size(); i++)
                    exceptions.get(i).printStackTrace();
            }
        }
    }

    public void startSimpleDB(final File queryFile,
            final Map<String, Integer> expectedBagOfTuples,
            final Queue<String> expectedOrderdTuples, final int numTuples)
            throws Throwable {
        // final Thread mainThread = Thread.currentThread();
        TestThread workingThread = new TestThread(queryFile,
                expectedBagOfTuples, expectedOrderdTuples, numTuples);
        workingThread.start();
        workingThread.join();
        if (workingThread.exceptions.size() > 0)
            throw workingThread.exceptions.get(0);
    }

    public void assertBagEqual(Map<String, Integer> expected,
            Map<String, Integer> actual) {
        if (expected.size() != actual.size()) {
            System.out.println("Expected:");
            for (String k : expected.keySet()) {
                System.out.println(k + "->" + expected.get(k));
            }
            System.out.println("Actual:");
            for (String k : actual.keySet()) {
                System.out.println(k + "->" + actual.get(k));
            }
        }
        Assert.assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, Integer> e : expected.entrySet()) {
            int ev = e.getValue();
            int av = actual.get(e.getKey());
            if (ev != av) {

                System.err.println("Expected:");
                for (String k : expected.keySet()) {
                    System.err.println(k + "->" + expected.get(k));
                }
                System.err.println("Actual:");
                for (String k : actual.keySet()) {
                    System.err.println(k + "->" + actual.get(k));
                }
            }
            Assert.assertEquals(ev, av);
        }
    }

    public void assertListEqual(Queue<String> expected, Queue<String> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        Iterator<String> itE = expected.iterator();
        Iterator<String> itA = actual.iterator();

        while (itE.hasNext()) {
            itA.hasNext();
            Assert.assertTrue(itE.next().equals(itA.next()));
        }
    }

    public static class ServerTestThread extends Thread {
        public ServerTestThread(ClassLoader cl, File queryFile, File dataDir,
                File confDir) {
            this.cl = cl;
            this.queryFile = queryFile;
            this.dataDir = dataDir;
            this.confDir = confDir;
        }

        ClassLoader cl;
        File queryFile;
        File dataDir;
        File confDir;

        public final ConcurrentHashMap<String, Integer> bagOfTuples = new ConcurrentHashMap<String, Integer>();
        public final ConcurrentLinkedQueue<String> orderedTuples = new ConcurrentLinkedQueue<String>();

        public volatile int numTuples = 0;

        public void run() {
            try {
                Class<?> serverC = cl.loadClass("simpledb.parallel.Server");
                String[] args = new String[5];
                args[0] = dataDir.getAbsolutePath() + "/test.schema";
                args[1] = "--conf";
                args[2] = confDir.getAbsolutePath();
                args[3] = "-f";
                args[4] = queryFile.getAbsolutePath();
                System.out.println("Server will start");
                serverC.getMethod("main", new Class[] { String[].class })
                        .invoke(null, (Object) args);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    ServerTestThread startServer(final File queryFile)
            throws InterruptedException {
        final ClassLoader cl = new LocalClassLoader(
                new String[] { binDir.getAbsolutePath() }, this.getClass()
                        .getClassLoader());
        ServerTestThread serverThread = new ServerTestThread(cl, queryFile,
                dataDir, confDir);
        serverThread.setDaemon(false);
        serverThread.setContextClassLoader(cl);
        serverThread.start();
        return serverThread;
    }

    static int nw = 0;

    Thread[] startWorkers() {

        Thread[] workerThreads = new Thread[2];
        for (int i = 0; i < 2; i++) {
            final int j = i;
            final ClassLoader cl = new LocalClassLoader(
                    new String[] { binDir.getAbsolutePath() }, this.getClass()
                            .getClassLoader());
            workerThreads[i] = new Thread() {
                public void run() {
                    try {
                        Class<?> workerC = cl
                                .loadClass("simpledb.parallel.Worker");
                        String[] args = new String[4];
                        args[0] = workers[j].getId();
                        args[1] = server.getId();
                        args[2] = "--data";
                        args[3] = dataDir.getAbsolutePath();
                        workerC.getMethod("main",
                                new Class[] { String[].class }).invoke(null,
                                (Object) args);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            workerThreads[i].setDaemon(false);
            workerThreads[i].setContextClassLoader(cl);
            workerThreads[i].start();
            System.out.println(++nw + " workers started");
        }
        return workerThreads;
    }

}

class LocalClassLoader extends ClassLoader {

    static class ClassPathItem {

        ClassPathItem(File binFolder) throws MalformedURLException {
            this.binFolder = binFolder;
        }

        ClassPathItem(JarResources jar) {
            this.jar = jar;
        }

        boolean isJar() {
            return this.jar != null;
        }

        File binFolder = null;
        JarResources jar = null;
    }

    protected ArrayList<ClassPathItem> classpaths = new ArrayList<ClassPathItem>();
    protected ClassLoader parent;

    /**
     * load the classes using the classpaths first, if not found, use the parent
     * */
    public LocalClassLoader(String[] classpaths, ClassLoader parent) {
        super(null);
        this.parent = parent;
        try {
            for (String classpath : classpaths)
                if (classpath.endsWith(".jar"))
                    this.classpaths.add(new ClassPathItem(new JarResources(
                            classpath)));
                else {
                    File libOrBinD;
                    if (classpath.equals("*"))
                        libOrBinD = new File(".");
                    else if (classpath.endsWith("/*"))
                        libOrBinD = new File(classpath.substring(0,
                                classpath.length() - 1));
                    else
                        libOrBinD = new File(classpath);
                    if (libOrBinD.isDirectory()) {
                        if (classpath.endsWith("*")) {
                            File[] subs = libOrBinD.listFiles();
                            if (subs != null) {
                                for (File f : subs) {
                                    if (f.getName().toLowerCase()
                                            .endsWith(".jar"))
                                        this.classpaths.add(new ClassPathItem(
                                                new JarResources(f
                                                        .getAbsolutePath())));
                                }
                            }
                        } else
                            this.classpaths.add(new ClassPathItem(libOrBinD));
                    }
                }

        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    protected Class<?> findClass(String name) {
        String resourceName = name.replace('.', '/') + ".class";
        byte buf[] = null;
        Class<?> cl;

        for (ClassPathItem cp : this.classpaths) {
            if (cp.isJar())
                buf = cp.jar.getResource(resourceName);
            else {
                try {
                    buf = readFileAll(
                            cp.binFolder.getAbsolutePath() + "/" + resourceName)
                            .array();
                } catch (Throwable e) {
                }
            }
            if (buf != null) {
                cl = defineClass(name, buf, 0, buf.length);
                return cl;
            }
        }
        try {
            if (parent != null)
                return parent.loadClass(name);
        } catch (ClassNotFoundException e) {
        }
        return null;
    }

    public static ByteBuffer readFileAll(String filePath) throws IOException {
        File f = new File(filePath);
        if (!f.isFile())
            return null;

        FileInputStream in = new FileInputStream(f);
        FileChannel fc = in.getChannel();
        ByteBuffer result = ByteBuffer.allocate((int) fc.size());
        fc.read(result);
        return result;
    }
}

/**
 * This class is from JavaWorld at:
 * http://www.javaworld.com/javaworld/javatips/jw-javatip49.html
 * 
 * It's free of use according to the editor:
 * http://www.javaworld.com/community/node/8181
 * 
 * The author should be Arthur Choi from IBM.
 * */
class JarResources {

    // external debug flag
    public boolean debugOn = false;

    // jar resource mapping tables
    private Hashtable<String, Integer> htSizes = new Hashtable<String, Integer>();
    private Hashtable<String, byte[]> htJarContents = new Hashtable<String, byte[]>();

    // a jar file
    private String jarFileName;

    /**
     * creates a JarResources. It extracts all resources from a Jar into an
     * internal hashtable, keyed by resource names.
     * 
     * @param jarFileName
     *            a jar or zip file
     */
    public JarResources(String jarFileName) {
        this.jarFileName = jarFileName;
        init();
    }

    /**
     * Extracts a jar resource as a blob.
     * 
     * @param name
     *            a resource name.
     */
    public byte[] getResource(String name) {
        return (byte[]) htJarContents.get(name);
    }

    /**
     * initializes internal hash tables with Jar file resources.
     */
    private void init() {
        try {
            // extracts just sizes only.
            ZipFile zf = new ZipFile(jarFileName);
            Enumeration<? extends ZipEntry> e = zf.entries();
            while (e.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) e.nextElement();
                if (debugOn) {
                    System.out.println(dumpZipEntry(ze));
                }
                htSizes.put(ze.getName(), new Integer((int) ze.getSize()));
            }
            zf.close();

            // extract resources and put them into the hashtable.
            FileInputStream fis = new FileInputStream(jarFileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            ZipInputStream zis = new ZipInputStream(bis);
            ZipEntry ze = null;
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory()) {
                    continue;
                }
                if (debugOn) {
                    System.out.println("ze.getName()=" + ze.getName() + ","
                            + "getSize()=" + ze.getSize());
                }
                int size = (int) ze.getSize();
                // -1 means unknown size.
                if (size == -1) {
                    size = ((Integer) htSizes.get(ze.getName())).intValue();
                }
                byte[] b = new byte[(int) size];
                int rb = 0;
                int chunk = 0;
                while (((int) size - rb) > 0) {
                    chunk = zis.read(b, rb, (int) size - rb);
                    if (chunk == -1) {
                        break;
                    }
                    rb += chunk;
                }
                // add to internal resource hashtable
                htJarContents.put(ze.getName(), b);
                if (debugOn) {
                    System.out.println(ze.getName() + "  rb=" + rb + ",size="
                            + size + ",csize=" + ze.getCompressedSize());
                }
            }
        } catch (NullPointerException e) {
            System.out.println("done.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Dumps a zip entry into a string.
     * 
     * @param ze
     *            a ZipEntry
     */
    private String dumpZipEntry(ZipEntry ze) {
        StringBuffer sb = new StringBuffer();
        if (ze.isDirectory()) {
            sb.append("d ");
        } else {
            sb.append("f ");
        }
        if (ze.getMethod() == ZipEntry.STORED) {
            sb.append("stored   ");
        } else {
            sb.append("defalted ");
        }
        sb.append(ze.getName());
        sb.append("\t");
        sb.append("" + ze.getSize());
        if (ze.getMethod() == ZipEntry.DEFLATED) {
            sb.append("/" + ze.getCompressedSize());
        }
        return (sb.toString());
    }

}
