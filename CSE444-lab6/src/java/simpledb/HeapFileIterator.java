package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator{
	
	
	private TransactionId tid;
	private HeapFile f;
	private int pgNum;
	private Iterator<Tuple> it;
	public HeapFileIterator(TransactionId tid, HeapFile f)
	{
		this.tid = tid;
		this.f = f;
	}
	
    private List<Tuple> getTupleLsFrPg(int pgNum) throws TransactionAbortedException, DbException{
        
        PageId pageId = new HeapPageId(f.getId(), pgNum);
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                        
        List<Tuple> tupleList = new ArrayList<Tuple>();
        
        // get all tuples from the first page in the file
        HeapPage hp = (HeapPage)page;
        Iterator<Tuple> itr = hp.iterator();
        while(itr.hasNext()){
            tupleList.add(itr.next());
        }
        return  tupleList;
    }
    
    @Override
	public void open() throws DbException, TransactionAbortedException
	{
		pgNum = 0;
		it = getTupleLsFrPg(pgNum).iterator();
	}
	
    @Override
	public boolean hasNext() throws DbException, TransactionAbortedException
	{
		if (it == null)
			return false;
		else
		{
			if (it.hasNext())
				return true;
			else
			{
				if (pgNum < f.numPages() - 1)
				{
					if (getTupleLsFrPg(pgNum+1).size() > 0)
						return true;
					else
						return false;
				}
				else
					return false;
			}
		}
	}
	
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,NoSuchElementException
    {
    	if (it == null)
    		throw new NoSuchElementException("tuple is null");
    	
    	if (it.hasNext())
    	{
    		Tuple i = it.next();
    		return i;
    	}
    	else
    	{
    		if (!it.hasNext() && pgNum < f.numPages() - 1)
    		{
    			pgNum++;
    			it = getTupleLsFrPg(pgNum).iterator();
    			if (it.hasNext())
    				return it.next();
    			else
    				throw new NoSuchElementException("No more Tuples");
    		}
    		else
    		{
    			throw new NoSuchElementException("No more Tuples");
    		}
    	}
    }
    
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();

    }
    
    @Override
    public void close() {
        it = null;

    }
}
