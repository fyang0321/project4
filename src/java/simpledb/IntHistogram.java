package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int[] mbuckets;
    int numBucket;
    int minBucket;  //left value of the min bucket
    int maxBucket;  //left value of the max bucket
    int bwidth;
    int numValue;
    
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.numBucket = buckets;
        mbuckets = new int[numBucket];
        minBucket = min;
        bwidth = (int) Math.ceil((double) (max - min) / numBucket);
        maxBucket = minBucket + (numBucket - 1) * bwidth;
        numValue = 0;
        
        for(int i = 0; i < numBucket; i++)
            mbuckets[i] = 0;
        
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int i;
        i = (int)Math.floor((double) (v - minBucket) / bwidth);
        if(i < 0 || i >= numBucket)
            return;
        mbuckets[i]++;
        numValue++;
        
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        //return -1.0;
        
        //equality expression
        if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE){
            double w =(double) bwidth;
            int i = (int)Math.floor((double) (v - minBucket) / bwidth);
            if(i < 0 || i >= numBucket)
                return 0.0;
            double h =(double) mbuckets[i];
            double ntups =(double) numValue;
            
            return (h/w)/ntups;
        }
        else if(op == Predicate.Op.GREATER_THAN){
            double w_b =(double) bwidth;
            int i = (int) Math.floor((double) (v - minBucket) / bwidth);
            if(i >= numBucket)
                return 0.0;
            else if(i < 0)
                return 1.0;
            double h_b =(double) mbuckets[i];
            double ntups =(double) numValue;
            double b_f = h_b/ntups;
            int b_right = (minBucket + (i + 1) * bwidth - 1);
            double b_part = (double)(b_right - v) / w_b;
            
            double result = b_f * b_part;       //selectivity of bucket b contributed to global
            
            for(int j = i + 1; j < numBucket; j++)  //the greater than part
                result += (double)mbuckets[j] / ntups;
            
            return result;
        }
        else if(op == Predicate.Op.LESS_THAN){
            double w_b =(double) bwidth;
            int i = (int) Math.floor((double) (v - minBucket) / bwidth);
            if(i >= numBucket)
                return 1.0;
            else if(i < 0)
                return 0.0;
            double h_b =(double) mbuckets[i];
            double ntups =(double) numValue;
            double b_f = h_b/ntups;
            int b_left = (minBucket + i * bwidth);
            double b_part = (double)(v - b_left) / w_b;
            
            double result = b_f * b_part;       //selectivity of bucket b contributed to global
            
            for(int j = 0; j < i; j++)  //the greater than part
                result += (double)mbuckets[j] / ntups;
            
            return result;
        }
        //opsite of greater than
        else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            double w_b =(double) bwidth;
            int i = (int) Math.floor((double) (v - minBucket) / bwidth);
            if(i >= numBucket)
                return 1.0;
            else if(i < 0)
                return 0.0;
            double h_b =(double) mbuckets[i];
            double ntups =(double) numValue;
            double b_f = h_b/ntups;
            int b_right = (minBucket + (i + 1) * bwidth - 1);
            double b_part = (double)(b_right - v) / w_b;
            
            double result = b_f * b_part;       //selectivity of bucket b contributed to global
            
            for(int j = i + 1; j < numBucket; j++)  //the greater than part
                result += (double)mbuckets[j] / ntups;
            
            return 1.0 - result;
        }
        else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            double w_b =(double) bwidth;
            int i = (int) Math.floor((double) (v - minBucket) / bwidth);
            if(i >= numBucket)
                return 0.0;
            else if(i < 0)
                return 1.0;
            double h_b =(double) mbuckets[i];
            double ntups =(double) numValue;
            double b_f = h_b/ntups;
            int b_left = (minBucket + i * bwidth);
            double b_part = (double)(v - b_left) / w_b;
            
            double result = b_f * b_part;       //selectivity of bucket b contributed to global
            
            for(int j = 0; j < i; j++)  //the greater than part
                result += (double)mbuckets[j] / ntups;
            
            return 1.0 - result;
        }
        //NOT_EQUALS
        else{
            double w =(double) bwidth;
            int i = (int) Math.floor((double) (v - minBucket) / bwidth);
            if(i < 0 || i >= numBucket)
                return 1.0;
            double h =(double) mbuckets[i];
            double ntups =(double) numValue;
            
            return 1.0 - (h/w)/ntups;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        //return null;
        String result = "Bucket Info:\n";
        for(int i = 0; i < numBucket; i++)
            result += "bucket" + i + ": " + mbuckets[i] +"\n";
        return result;
    }
}
