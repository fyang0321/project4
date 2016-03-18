package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    Predicate F_predicate;
    DbIterator F_child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.F_predicate = p;
        this.F_child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.F_predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return F_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        F_child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        F_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        F_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(this.F_child == null)
            return null;

        try {
            while(this.F_child.hasNext()){
                Tuple ttuple = F_child.next();
                if(F_predicate.filter(ttuple))
                    return ttuple;
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (DbException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
            System.exit(0);
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {F_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        F_child = children[0];
    }

}