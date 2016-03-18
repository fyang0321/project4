package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    int iocostperpage;
    int numTuple;
    HashMap<String, Integer> maxValue;
    HashMap<String, Integer> minValue;
    HashMap<String, IntHistogram> ihistogram;
    HashMap<String, StringHistogram> shistogram;
    DbFile mdbfile;
    TupleDesc td;
    
    

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        
        this.iocostperpage = ioCostPerPage;
        numTuple = 0;
        
        maxValue = new HashMap<String, Integer>();
        minValue = new HashMap<String, Integer>();
        ihistogram = new HashMap<String, IntHistogram>();
        shistogram = new HashMap<String, StringHistogram>();
        
        mdbfile = Database.getCatalog().getDbFile(tableid);
        td = Database.getCatalog().getTupleDesc(tableid);
        
        //init histogram
        //scan once, for each field, find the min and max
        DbFileIterator it = mdbfile.iterator(new TransactionId());
        Tuple tp;
        
        try{
            it.open();
            while(it.hasNext()){
                tp = it.next();
                numTuple++;
                for(int i = 0; i < td.numFields(); i++){
                    if(td.getFieldType(i) == Type.INT_TYPE){
                        IntField tfield = (IntField)tp.getField(i);
                        //max
                        if(maxValue.get(td.getFieldName(i)) == null)
                            maxValue.put(td.getFieldName(i), tfield.getValue());
                        else{
                            if(tfield.getValue() > maxValue.get(td.getFieldName(i)))
                                maxValue.put(td.getFieldName(i), tfield.getValue());
                        }
                        //min
                        if(minValue.get(td.getFieldName(i)) == null)
                        minValue.put(td.getFieldName(i), tfield.getValue());
                        else{
                            if(tfield.getValue() < minValue.get(td.getFieldName(i)))
                                minValue.put(td.getFieldName(i), tfield.getValue());
                        }
                    }
                    //no need for string histogram
                }
            }
        
            //create histogram
            for(int i = 0; i < td.numFields(); i++){
                String fieldname = td.getFieldName(i);
                if(td.getFieldType(i) == Type.INT_TYPE){
                    IntHistogram tih = new IntHistogram(NUM_HIST_BINS, minValue.get(fieldname), maxValue.get(fieldname));
                    ihistogram.put(fieldname, tih);
                }
                else{
                    StringHistogram tsh = new StringHistogram(NUM_HIST_BINS);
                    shistogram.put(fieldname, tsh);
                }
            }
        
            //add values
            it.rewind();
            while(it.hasNext()){
                tp = it.next();
                for(int i = 0; i < td.numFields(); i++){
                    if(td.getFieldType(i) == Type.INT_TYPE){
                        IntField tfield = (IntField)tp.getField(i);
                        ihistogram.get(td.getFieldName(i)).addValue(tfield.getValue());
                    }
                    else{
                        StringField tfield = (StringField)tp.getField(i);
                        shistogram.get(td.getFieldName(i)).addValue(tfield.getValue());
                    }
                }
            }
        
            it.close();
        }
        catch (DbException e){
            e.printStackTrace();
        }
        catch (TransactionAbortedException e){
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ((HeapFile)mdbfile).numPages() * iocostperpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(numTuple * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //return 1.0;
        if(constant.getType() == Type.INT_TYPE){
            IntHistogram ihg = ihistogram.get(td.getFieldName(field));
            int v = ((IntField)constant).getValue();
            return ihg.estimateSelectivity(op, v);
        }
        else{
            StringHistogram shg = shistogram.get(td.getFieldName(field));
            String v = ((StringField)constant).getValue();
            return shg.estimateSelectivity(op, v);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuple;
    }

}