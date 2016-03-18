package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
	private List<Tuple> tuples = null;	
	private Iterator<Tuple> it = null;

	public HeapFileIterator(List<Tuple> tuples) {
		this.tuples = tuples;
	}

	    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException {
    	this.it = this.tuples.iterator();
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
    	return it == null ? false : it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    	if (it == null)
    		throw new NoSuchElementException("Iterator is not open");

    	Tuple tuple = null;
    	try {
    		tuple = it.next();
    	} catch(NoSuchElementException e) {
    		e.printStackTrace();
    		System.exit(0);
    	}

    	return tuple;
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    	this.it = null;
    }
}


