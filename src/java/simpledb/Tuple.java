package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc = null;
    private RecordId recordId = null;
    List<Field> tupleFields = null;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleFields = new ArrayList<Field>();
        //Document: add dummy field for setField() method
        for (int i = 0; i < td.numFields(); i++) {
            Field dummy = new IntField(-1);
            tupleFields.add(dummy);
        }
        tupleDesc = td;
    }

    //A static method to merge tuples as merge method in TupleDesc class
    public static Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc td = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());

        Tuple tuple = new Tuple(td);

        int currentIdx = 0;
        Iterator<Field> it1 = t1.fields(), it2 = t2.fields();
        while (it1.hasNext()) {
            tuple.setField(currentIdx++, it1.next());
        }

        while (it2.hasNext()) {
            tuple.setField(currentIdx++, it2.next());
        }

        return tuple;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // return null;
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        // return null;
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = new RecordId(rid.getPageId(), rid.tupleno());
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= this.tupleFields.size())
            throw new IndexOutOfBoundsException("Field " + i + " is out of fields bound");

        this.tupleFields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        // return null;
        if (i > this.tupleFields.size() - 1)
            throw new IndexOutOfBoundsException(i + "is out of bounds");

        return tupleFields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        StringBuilder sb = new StringBuilder();
        int size = tupleFields.size();
        for (int i = 0; i < size; i++) {
            Field f = tupleFields.get(i);
            String dilimter = i == size-1 ? "\n" : "\t";
            if (f == null)
                sb.append("null");
            else
                sb.append(f.toString());

            sb.append(dilimter);
        }
 
        return sb.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        // return null;
        return tupleFields.iterator();
    }
}
