package simpledb.execution;

import java.util.*;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    // Use Object as key: Field or null (for NO_GROUPING)
    private final Map<Field, Integer> aggregateValues;
    private final Map<Field, Integer> counts;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield; 
        this.what = what;
        this.aggregateValues = new HashMap<>();
        this.counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField aggField = (IntField) tup.getField(afield);
        int value = aggField.getValue();

        // Initialize if not present
        if (!aggregateValues.containsKey(groupField)) {
            switch (what) {
                case MIN: 
                    aggregateValues.put(groupField, value); 
                    break;
                case MAX: 
                    aggregateValues.put(groupField, value); 
                    break;
                case SUM: 
                    aggregateValues.put(groupField, value); 
                    break;
                case AVG: 
                    aggregateValues.put(groupField, value); 
                    break;
                case COUNT: 
                    aggregateValues.put(groupField, 1); 
                    break;
                default: 
                    throw new UnsupportedOperationException("Op not supported");
            }
            counts.put(groupField, 1);
        } else {
            switch (what) {
                case MIN:
                    aggregateValues.put(groupField, Math.min(aggregateValues.get(groupField), value));
                    break;
                case MAX:
                    aggregateValues.put(groupField, Math.max(aggregateValues.get(groupField), value));
                    break;
                case SUM:
                    aggregateValues.put(groupField, aggregateValues.get(groupField) + value);
                    break;
                case AVG:
                    aggregateValues.put(groupField, aggregateValues.get(groupField) + value);
                    counts.put(groupField, counts.get(groupField) + 1);
                    break;
                case COUNT:
                    aggregateValues.put(groupField, aggregateValues.get(groupField) + 1);
                    break;
                default:
                    throw new UnsupportedOperationException("Op not supported");
            }
            if (what != Op.COUNT && what != Op.AVG) {
                counts.put(groupField, counts.get(groupField) + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    @Override
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            for (Field group : aggregateValues.keySet()) {
                int aggVal;
                if (what == Op.AVG) {
                    aggVal = aggregateValues.get(group) / counts.get(group);
                } else {
                    aggVal = aggregateValues.get(group);
                }
                Tuple tup = new Tuple(td);
                tup.setField(0, new IntField(aggVal));
                tuples.add(tup);
            }
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for (Field group : aggregateValues.keySet()) {
                int aggVal;
                if (what == Op.AVG) {
                    aggVal = aggregateValues.get(group) / counts.get(group);
                } else {
                    aggVal = aggregateValues.get(group);
                }
                Tuple tup = new Tuple(td);
                tup.setField(0, group);
                tup.setField(1, new IntField(aggVal));
                tuples.add(tup);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
