package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    DbIterator inIT;
    DbIterator outIT;
    int aField;
    int gField;
    Aggregator.Op Aop;
    
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    // some code goes here
        this.inIT = child;
        this.aField = afield;
        this.gField = gfield;
        this.Aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    // some code goes here
        return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    // some code goes here
        if(this.gField == Aggregator.NO_GROUPING)
            return null;
        else
            return this.inIT.getTupleDesc().getFieldName(this.gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    // some code goes here
        return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    // some code goes here
        return this.inIT.getTupleDesc().getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    // some code goes here
        return this.Aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
        TransactionAbortedException {
    // some code goes here
        super.open();
        inIT.open();
        Aggregator tagg = null;
        
        //determine the type of aggregator
        if(inIT.getTupleDesc().getFieldType(aField).equals(Type.INT_TYPE)){
            if(gField == Aggregator.NO_GROUPING)
                tagg = new IntegerAggregator(gField, null, aField, Aop);
            else
                tagg = new IntegerAggregator(gField, inIT.getTupleDesc().getFieldType(gField), aField, Aop);
        }
        else{
            if(gField == Aggregator.NO_GROUPING)
                tagg = new StringAggregator(gField, null, aField, Aop);
            else
                tagg = new StringAggregator(gField, inIT.getTupleDesc().getFieldType(gField), aField, Aop);
        }
        
        //merge tuples
        while(inIT.hasNext()){
            tagg.mergeTupleIntoGroup(inIT.next());
        }
        
        outIT = tagg.iterator();
        outIT.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    // some code goes here
        if(outIT.hasNext())
            return outIT.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    // some code goes here
        inIT.rewind();
        outIT.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    // some code goes here
        TupleDesc td = null;
        TupleDesc originalTD = inIT.getTupleDesc();
        Type[] typeAr;
        String[] fieldAr;
        
        //no group by
        if(gField == Aggregator.NO_GROUPING){
            typeAr = new Type[]{originalTD.getFieldType(aField)};
            fieldAr = new String[]{Aop.toString() + " (" + originalTD.getFieldName(aField) + ")"};
            td = new TupleDesc(typeAr, fieldAr);
        }
        else{
            typeAr = new Type[]{originalTD.getFieldType(gField), originalTD.getFieldType(aField)};
            fieldAr = new String[]{originalTD.getFieldName(gField), Aop.toString() + " (" + originalTD.getFieldName(aField) + ")"};
            td = new TupleDesc(typeAr, fieldAr);
        }
        
        return td;
    }

    public void close() {
    // some code goes here
        super.close();
        inIT.close();
        outIT.close();
    }

    @Override
    public DbIterator[] getChildren() {
    // some code goes here
        if(inIT == null)
            return null;
        else
            return new DbIterator[]{inIT};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    // some code goes here
        inIT = children[0];
    }
    
}