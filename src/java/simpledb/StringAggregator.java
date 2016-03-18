package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private Map<Field, Integer> fieldCountTable = null;
    private List<Tuple> groupedTuples = null;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, 
     *or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), 
     *or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator ONLY supports Count.");
        }

        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        fieldCountTable = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, 
     *grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by 
     *field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field keyField = (this.gbField != Aggregator.NO_GROUPING) ? 
                        tup.getField(this.gbField) : null;

        if (!fieldCountTable.containsKey(keyField)) {
            fieldCountTable.put(keyField, 0);
        }

        fieldCountTable.put(keyField, fieldCountTable.get(keyField) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        groupedTuples = new ArrayList<Tuple>();

        //create tuple description for aggregate group
        TupleDesc td = null;
        List<String> names = new LinkedList<String>();
        List<Type> types = new LinkedList<Type>();

        names.add("aggregateVal");
        types.add(Type.INT_TYPE);

        if (this.gbField != Aggregator.NO_GROUPING) {
            names.add(0, "groupVal");
            types.add(0, this.gbFieldType);
        }

        td = new TupleDesc(types.toArray(new Type[0])
                        ,names.toArray(new String[0]));

        for (Field field : fieldCountTable.keySet()) {
            Tuple newTuple = new Tuple(td);

            if (this.gbField != Aggregator.NO_GROUPING) {
                newTuple.setField(0, field);
                newTuple.setField(1, new IntField(fieldCountTable.get(field)));
            } else {
                newTuple.setField(0, new IntField(fieldCountTable.get(field)));
            }

            groupedTuples.add(newTuple);
        }

        return new TupleIterator(td, groupedTuples);
    }

}
