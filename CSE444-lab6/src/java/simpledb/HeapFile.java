package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	private File f;
	private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return this.td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	try{
    		RandomAccessFile rf = new RandomAccessFile(f,"r");
    		int pos = pid.pageNumber() * Database.getBufferPool().PAGE_SIZE;
    		byte[] b = new byte[Database.getBufferPool().PAGE_SIZE];
    		rf.seek(pos);
    		rf.read(b, 0, Database.getBufferPool().PAGE_SIZE);
    		
    		HeapPageId hpid = (HeapPageId)pid;
    		rf.close();
    		return new HeapPage(hpid,b);
    	}catch (IOException e)
    	{
    		e.printStackTrace();
    	}
    	throw new IllegalArgumentException();
        //return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	try{
    		RandomAccessFile rf = new RandomAccessFile(f,"rws");
    		PageId pid = page.getId();
    		int pos = pid.pageNumber() * Database.getBufferPool().PAGE_SIZE;
    		byte[] b = new byte[Database.getBufferPool().PAGE_SIZE];
    		b = page.getPageData();
    		rf.seek(pos);
    		rf.write(b, 0, Database.getBufferPool().PAGE_SIZE);
    		
    		rf.close();
    	}
    	catch(IOException e)
    	{
    		e.printStackTrace();
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length() / Database.getBufferPool().PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> affected = new ArrayList<Page>();
    	try{
    		if (this.getEmptyPages(tid).isEmpty())
    		{
    			HeapPageId hpid = new HeapPageId(this.getId(),this.numPages());
    			HeapPage hp = new HeapPage(hpid, HeapPage.createEmptyPageData());
    			hp.insertTuple(t);
    			this.writePage(hp);
    			affected.add(hp);
    		}
    		else
    		{
    			Page p = this.getEmptyPages(tid).get(0);
    			HeapPage hp = (HeapPage)p;
    			hp.insertTuple(t);
    			affected.add(hp);
    		}
    	}
    	catch (DbException e)
    	{
    		e.printStackTrace();
    	}
    	catch (IOException e)
    	{
    		e.printStackTrace();
    	}
    	catch (TransactionAbortedException e)
    	{
    		e.printStackTrace();
    	}
    	
        return affected;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	RecordId rid = t.getRecordId();
    	ArrayList<Page> affected = new ArrayList<Page>();
    	try{
    		Page pg = Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    		HeapPage hpg = (HeapPage) pg;
    		hpg.deleteTuple(t);
    		affected.add(hpg);
    	}
    	catch (DbException e)
    	{
    		e.printStackTrace();
    	}
    	return affected;
        //return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    public ArrayList<Page> getEmptyPages(TransactionId tid) throws DbException, TransactionAbortedException{
    	ArrayList<Page> emptyPages = new ArrayList<Page>();
    	try{
    		int tableId =this.getId();
    		for (int i = 0; i < this.numPages(); ++i)
    		{
    			HeapPageId pid = new HeapPageId(tableId,i);
    			Page pg = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                if (((HeapPage)pg).getNumEmptySlots()!=0){
                    emptyPages.add(pg);
                }
    		}
    	}
    	catch (DbException e)
    	{
    		e.printStackTrace();
    	}
    	catch (TransactionAbortedException e)
    	{
    		e.printStackTrace();
    	}
    	return emptyPages;
    }
}

