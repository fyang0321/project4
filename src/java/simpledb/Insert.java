package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;


    private TransactionId transactionId = null;
    private DbIterator it = null;
    private int tableId;

    private boolean hasFetched = false;
    private TupleDesc tupleDesc = null;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        if (!Database.getCatalog().getTupleDesc(tableid)
                .equals(child.getTupleDesc()))
            throw new DbException("TupleDesc of child is different from the one of table.");

        this.transactionId = t;
        this.it = child;
        this.tableId = tableid;

        Type[] type = new Type[]{ Type.INT_TYPE };
        tupleDesc = new TupleDesc(type);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            super.open();
            it.open();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (DbException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void close() {
        // some code goes here
        it.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            it.rewind();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (DbException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple returnedTuple = null;

        if (!hasFetched) {        
            try {
                int count = 0;
                while (it.hasNext()) {
                    Database.getBufferPool().insertTuple(transactionId, tableId, it.next());
                    count++;
                }

                returnedTuple = new Tuple(tupleDesc);
                returnedTuple.setField(0, new IntField(count));
                hasFetched = true;
            } catch(DbException e) {
                e.printStackTrace();
                System.exit(0);
            } catch(TransactionAbortedException e) {
                e.printStackTrace();
                System.exit(0);
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        return returnedTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.it };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.it = children[0];
    }
}
