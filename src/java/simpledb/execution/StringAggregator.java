package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> counts;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT){
            throw new IllegalArgumentException("Illegal operator, only supports count");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.counts = new HashMap<>(); // Initialize the counts map
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        counts.put(groupField, counts.getOrDefault(groupField, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (gbfield == NO_GROUPING) { // if there is no grouping
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else { // if there is grouping
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
        for (Map.Entry<Field, Integer> entry : counts.entrySet()) { // iterate through the counts
            Field groupVal = entry.getKey(); // if there is grouping, this is the group value
            int aggregateVal = entry.getValue(); // this is the count for that group
            Tuple tup = new Tuple(td); // create a new tuple with the appropriate TupleDesc
            if (gbfield == NO_GROUPING) { // if there is no grouping
                tup.setField(0, new IntField(aggregateVal)); // set the aggregate value
            } else { // if there is grouping
                tup.setField(0, groupVal); // set the group value
                tup.setField(1, new IntField(aggregateVal)); // set the aggregate value
            }
            tuples.add(tup); // add the tuple to the list of tuples
        }
        return new TupleIterator(td, tuples); // return a new TupleIterator with the list of tuples and the TupleDesc
    }

}
