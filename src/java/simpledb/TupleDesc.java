package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return null;
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    //TDItems
    public List<TDItem> tdItems = null;
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        // some code goes here
        if (typeAr.length != fieldAr.length || typeAr.length < 1) {
            throw new IllegalArgumentException("It must contain at least one entry");
        }
        tdItems = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) throws IllegalArgumentException {
        // some code goes here
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("It must contain at least one entry");
        }
        tdItems = new ArrayList<TDItem>();
        for(Type t : typeAr) {
            TDItem item = new TDItem(t, "");
            tdItems.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return 0;
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        if (i > this.tdItems.size() - 1)
            throw new NoSuchElementException("No such element " + i);

        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        if (i > this.tdItems.size() - 1)
            throw new NoSuchElementException("No such element " + i);

        return this.tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        //return 0;
        if (name != null) {
            for (int i = 0; i < tdItems.size(); i++) {
                if (name.equals(tdItems.get(i).fieldName)) {
                    return i;
                }
            }
        }

        throw new NoSuchElementException("No such field with name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // return 0;
        int totalSize = 0;
        for (int i = 0; i < this.tdItems.size(); i++) {
            totalSize += this.getFieldType(i).getLen();
        }

        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //return null;
        int totalFields = td1.numFields() + td2.numFields();
        Type[] types = new Type[totalFields];
        String[] names = new String[totalFields];

        int currentIndex = 0, size1 = td1.numFields(), size2 = td2.numFields();
        for (int i = 0; i < size1; i++) {
            types[currentIndex] = td1.getFieldType(i);
            names[currentIndex] = td1.getFieldName(i);
            currentIndex++;
        }

        for (int i = 0; i < size2; i++) {
            types[currentIndex] = td2.getFieldType(i);
            names[currentIndex] = td2.getFieldName(i);
            currentIndex++;
        }

        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        // return false;
        if (!(o instanceof TupleDesc))
            return false;

        int size = this.numFields();
        TupleDesc other = (TupleDesc)o;
        if (other == null || size != other.numFields())
            return false;

        for (int i = 0; i < size; i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i)))
                return false;
        }

        return true;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        // return "";
        StringBuilder sb = new StringBuilder();
        int size = this.tdItems.size();
        for (int i = 0; i < size; i++) {
            sb.append(this.getFieldType(i));
            sb.append("(");
            sb.append(this.getFieldName(i));
            sb.append(")");
            if (i < size-1)
                sb.append(", ");
        }

        return sb.toString();
    }
}
