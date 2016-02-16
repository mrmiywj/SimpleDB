package simpledb;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import simpledb.TupleDesc.TDItem;
import simpledb.parallel.ParallelUtility;
import simpledb.parallel.Server;
import simpledb.parallel.SocketInfo;

/**
 * Split all the heapfiles within a database to #worker parts.
 * 
 * The split data files are placed under the data directory
 * of current working directory.
 * 
 * The data of a worker
 *    host:port
 * is placed under data/host_port
 * 
 * The catalog file of each split part is the same, namely catalog.schema
 * 
 * This class will be called in the startSimpleDB.sh script.
 * */
public class HeapFileSplitter {

    final static String usage = "Usage: HeapFileSplitter <catalogFile> [--conf <confDir>] [--output <outputFolder>]";
    
    public final static String DEFAULT_OUTPUT_DIR="data";

    public static void writeHeapFile(ArrayList<Tuple> tuples, File outFile,
            int npagebytes, Type[] typeAr) throws IOException {

        int nrecbytes = 0;
        for (int i = 0; i < typeAr.length; i++) {
            nrecbytes += typeAr[i].getLen();
        }
        int maxNumRecordsInAPage = (npagebytes * 8) / (nrecbytes * 8 + 1); // floor comes
                                                               // for free

        // per record, we need one bit; there are nrecords per page, so we need
        // nrecords bits, i.e., ((nrecords/32)+1) integers.
        int nheaderbytes = (maxNumRecordsInAPage / 8);
        if (nheaderbytes * 8 < maxNumRecordsInAPage)
            nheaderbytes++; // ceiling
        int nheaderbits = nheaderbytes * 8;

        FileOutputStream os = new FileOutputStream(outFile);

        int npages = 0;
        int numRecordInCurrPage = 0;
        int totalRecordCount = 0;

        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(
                nheaderbytes);
        DataOutputStream headerStream = new DataOutputStream(headerBAOS);
        ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
        DataOutputStream pageStream = new DataOutputStream(pageBAOS);

        for (Tuple t : tuples) {
            int fieldNo = 0;
            numRecordInCurrPage++;
            totalRecordCount++;
            Iterator<Field> it = t.fields();
            while (it.hasNext()) {
                Field f = it.next();
                if (typeAr[fieldNo] == Type.INT_TYPE) {
                    IntField i = (IntField) f;
                    pageStream.writeInt(i.getValue());
                } else if (typeAr[fieldNo] == Type.STRING_TYPE) {
                    StringField sf = (StringField) f;
                    String s = sf.getValue();
                    int overflow = Type.STRING_LEN - s.length();
                    if (overflow < 0) {
                        String news = s.substring(0, Type.STRING_LEN);
                        s = news;
                    }
                    pageStream.writeInt(s.length());
                    pageStream.writeBytes(s);
                    while (overflow-- > 0)
                        pageStream.write((byte) 0);
                }
                fieldNo++;
            }

            // if we wrote a full page of records, or if we're done altogether,
            // write out the header of the page.
            //
            // in the header, write a 1 for bits that correspond to records
            // we've
            // written and 0 for empty slots.
            //
            // when we're done, also flush the page to disk, but only if it has
            // records on it. however, if this file is empty, do flush an empty
            // page to disk.
            if (numRecordInCurrPage >= maxNumRecordsInAPage
                    || totalRecordCount == tuples.size() && numRecordInCurrPage > 0
                    || totalRecordCount == tuples.size() && npages == 0) {
                int i = 0;
                byte headerbyte = 0;

                for (i = 0; i < nheaderbits; i++) {
                    if (i < numRecordInCurrPage)
                        headerbyte |= (1 << (i % 8));

                    if (((i + 1) % 8) == 0) {
                        headerStream.writeByte(headerbyte);
                        headerbyte = 0;
                    }
                }

                if (i % 8 > 0)
                    headerStream.writeByte(headerbyte);

                // pad the rest of the page with zeroes

                for (i = 0; i < (npagebytes - (numRecordInCurrPage * nrecbytes + nheaderbytes)); i++)
                    pageStream.writeByte(0);

                // write header and body to file
                headerStream.flush();
                headerBAOS.writeTo(os);
                pageStream.flush();
                pageBAOS.writeTo(os);

                // reset header and body for next page
                headerBAOS = new ByteArrayOutputStream(nheaderbytes);
                headerStream = new DataOutputStream(headerBAOS);
                pageBAOS = new ByteArrayOutputStream(npagebytes);
                pageStream = new DataOutputStream(pageBAOS);

                numRecordInCurrPage = 0;
                npages++;
            }
        }
        os.close();
    }
    
    private static int getTotalTuples(int tableid)
    {
    	int count = 0;
        try {
        	Transaction t = new Transaction();
        	t.start();
        	SeqScan s = new SeqScan(t.getId(), tableid, null);
        	s.open();
        	while (s.hasNext()) {
        		s.next();	
        		count++;
        	}
        	t.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return count;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 5) {
            System.out.println("Invalid number of arguments.\n" + usage);
            return;
        }

        String confDir = Server.DEFAULT_CONF_DIR;
        String outputDir = DEFAULT_OUTPUT_DIR;
        if (args.length>=3 && args[1].equals("--conf"))
        {
            confDir = args[2];
            args=ParallelUtility.removeArg(args, 1);
            args=ParallelUtility.removeArg(args, 1);
        }
        
        if (args.length>=3 && args[1].equals("--output"))
        {
            outputDir = args[2];
            args=ParallelUtility.removeArg(args, 1);
            args=ParallelUtility.removeArg(args, 1);
        }
        
        Catalog c = Database.getCatalog();
        
        SocketInfo[] workers = ParallelUtility.loadWorkers(confDir);
        c.loadSchema(args[0]);
        TableStats.computeStatistics();

        File catalogFile = new File(args[0]);

        for (SocketInfo worker : workers) {
            File folder = new File(outputDir+"/" + worker.getHost() + "_" + worker.getPort());
            folder.mkdirs();
            ParallelUtility.copyFileFolder(catalogFile, new File(folder.getAbsolutePath()
                    + "/catalog.schema"),true);
        }

        TransactionId fateTid = new TransactionId();

        Iterator<Integer> tableIds = c.tableIdIterator();
        while (tableIds.hasNext()){
            int tableid = tableIds.next();
            int numTuples = getTotalTuples(tableid);
            HeapFile h = (HeapFile) c.getDatabaseFile(tableid);

            int eachSplitSize = numTuples / workers.length;
            int[] splitSizes = new int[workers.length];
            Arrays.fill(splitSizes, eachSplitSize);
            for (int i = 0; i < numTuples % workers.length; i++) {
                splitSizes[i] += 1;
            }

            DbFileIterator dfi = h.iterator(fateTid);
            dfi.open();

            for (int i = 0; i < workers.length; i++) {
                ArrayList<Tuple> buffer = new ArrayList<Tuple>();
                for (int j = 0; j < splitSizes[i]; j++) {
                    dfi.hasNext();
                    buffer.add(dfi.next());
                }
                Iterator<TDItem> items = h.getTupleDesc().iterator();
                ArrayList<Type> types = new ArrayList<Type>();
                while (items.hasNext())
                    types.add(items.next().fieldType);

                writeHeapFile(buffer, new File(outputDir+"/" + workers[i].getHost() + "_"
                        + workers[i].getPort() + "/" + c.getTableName(tableid) + ".dat"),
                        BufferPool.getPageSize(), types.toArray(new Type[]{}));
            }

        }

    }
}
