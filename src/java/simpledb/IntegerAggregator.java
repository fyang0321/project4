package simpledb;

import java.util.*;
import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    int IA_gbField;
    Type IA_gbFieldType;
    int IA_aField;
    Op IA_what;
    //for the convenience of AVG, 
    Map<Field, Integer> fieldCountTable = null;
    Map<Field, Integer> fieldSumTable = null;
    //the aggregaation function result
    Map<Field, Integer> fieldAggTable = null;
    
    //store the tupledesc of the input tuple
    TupleDesc originalTD = null;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.IA_gbField = gbfield;
        this.IA_gbFieldType = gbfieldtype;
        this.IA_aField = afield;
        this.IA_what = what;
        fieldCountTable = new HashMap<Field, Integer>();
        fieldSumTable = new HashMap<Field, Integer>();
        fieldAggTable = new HashMap<Field, Integer>();
        originalTD = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        
        if(originalTD == null)
            originalTD = tup.getTupleDesc();
        
        Field tupGroupField = null;
        IntField tupAggreField = null;
        boolean newgroup;
        
        if(this.IA_gbField != Aggregator.NO_GROUPING)
            tupGroupField = tup.getField(this.IA_gbField);
        else
            tupGroupField = new IntField(Aggregator.NO_GROUPING);
        
        tupAggreField = (IntField) tup.getField(IA_aField);
        
        newgroup = !fieldAggTable.containsKey(tupGroupField);
        
        if(IA_what == Aggregator.Op.COUNT){
            if(newgroup)
                fieldAggTable.put(tupGroupField, 1);
            else
                fieldAggTable.put(tupGroupField, fieldAggTable.get(tupGroupField) + 1);
        }
        else if(IA_what == Aggregator.Op.SUM){
            if(newgroup)
                fieldAggTable.put(tupGroupField, tupAggreField.getValue());
            else
                fieldAggTable.put(tupGroupField, fieldAggTable.get(tupGroupField) + tupAggreField.getValue());
        }
        else if(IA_what == Aggregator.Op.MIN){
            if(newgroup)
                fieldAggTable.put(tupGroupField, tupAggreField.getValue());
            else{
                if(tupAggreField.getValue() < fieldAggTable.get(tupGroupField))
                    fieldAggTable.put(tupGroupField, tupAggreField.getValue());     
            }
        }
        else if(IA_what == Aggregator.Op.MAX){
            if(newgroup)
                fieldAggTable.put(tupGroupField, tupAggreField.getValue());
            else{
                if(tupAggreField.getValue() > fieldAggTable.get(tupGroupField))
                    fieldAggTable.put(tupGroupField, tupAggreField.getValue());     
            }
        }
        else if(IA_what == Aggregator.Op.AVG){
            if(newgroup)
            {
                fieldAggTable.put(tupGroupField, tupAggreField.getValue());
                fieldCountTable.put(tupGroupField, 1);
                fieldSumTable.put(tupGroupField, tupAggreField.getValue());
            }
            else{
                fieldCountTable.put(tupGroupField, fieldCountTable.get(tupGroupField) + 1);
                fieldSumTable.put(tupGroupField, fieldSumTable.get(tupGroupField) + tupAggreField.getValue());
                Integer avgnum = fieldSumTable.get(tupGroupField) / fieldCountTable.get(tupGroupField);
                fieldAggTable.put(tupGroupField, avgnum);
            }
        }
        else{
            //Do Nothing
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> groupedTuples = new ArrayList<Tuple>();
        
        TupleDesc td = null;
        Type[] typeAr;
        String[] fieldAr;
        //a single aggregateVal
        if(this.IA_gbField == Aggregator.NO_GROUPING){
            typeAr = new Type[]{Type.INT_TYPE};
            fieldAr = new String[]{originalTD.getFieldName(IA_aField)};
            td = new TupleDesc(typeAr, fieldAr);
        }
        //pair (groupVal, aggregateVal)
        else{
            typeAr = new Type[]{IA_gbFieldType, Type.INT_TYPE};
            fieldAr = new String[]{originalTD.getFieldName(IA_gbField), originalTD.getFieldName(IA_aField)};
            td = new TupleDesc(typeAr, fieldAr);
        }
        
        //load tuple
        for(Field gfield:fieldAggTable.keySet()){
            Tuple newTuple = new Tuple(td);
            Field afield =new IntField(fieldAggTable.get(gfield));
            
            if(this.IA_gbField == Aggregator.NO_GROUPING){
                newTuple.setField(0, afield);
            }
            else{
                newTuple.setField(0, gfield);
                newTuple.setField(1, afield);
            }
            
            groupedTuples.add(newTuple);
        }
        
        return new TupleIterator(td, groupedTuples);
        
    }

}