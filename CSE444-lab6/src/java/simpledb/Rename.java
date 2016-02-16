package simpledb;

import java.util.Iterator;

import simpledb.TupleDesc.TDItem;

/**
 * Only used in lab6.
 * */
public class Rename extends Operator {

    private static final long serialVersionUID = 1L;
    private int inField;
    private String newName;
    private DbIterator child;
    private TupleDesc td;

    public Rename(int inField, String newName, DbIterator child) {
        this.inField = inField;
        this.newName = newName;
        this.child = child;
        updateTD();
    }

    public String newName()
    {
        return this.newName;
    }
    
    public int renamedField()
    {
        return inField;
    }
    
    @Override
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }
    
    public void close()
    {
        super.close();
        this.child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (child.hasNext())
        {
            Tuple n = child.next();
            n.resetTupleDesc(td);
            return n;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
        updateTD();
    }

    @Override
    public TupleDesc getTupleDesc() {

        return this.td;
    }
    
    private void updateTD()
    {
        TupleDesc childTD = child.getTupleDesc();
        Type[] types = new Type[childTD.numFields()];
        String[] names = new String[childTD.numFields()];

        int i = 0;
        Iterator<TupleDesc.TDItem> it = childTD.iterator();
        while (it.hasNext()) {
            TDItem item = it.next();
            types[i] = item.fieldType;
            if (i == inField)
                names[i] = newName;
            else
                names[i] = item.fieldName;
            i++;
        }
        td = new TupleDesc(types,names);
    }

}
