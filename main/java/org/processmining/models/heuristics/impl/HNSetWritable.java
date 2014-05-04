package org.processmining.models.heuristics.impl;

import com.galaev.mapreduce.geneticminer.writables.arrays.HNSubSetArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Ordered set of <code>HNSubSet</code> objects that is used to represent sets
 * in the input and output sets of <code>HeuristicsNet</code> objects.
 *
 * @author Peter van den Brand and Ana Karla Alves de Medeiros
 *
 */
public class HNSetWritable implements Writable {

    private HNSubSetWritable[] set;
    private int size;

    /**
     * Constructs a <code>HNSetWritable</code> object
     */
    public HNSetWritable() {
        set = new HNSubSetWritable[10];
        size = 0;

    }

    // this constructor is only used by deepCopy
    private HNSetWritable(HNSetWritable setToCopy) {
        set = new HNSubSetWritable[setToCopy.set.length];
        size = setToCopy.size;

        for (int i = 0; i < size; i++) {
            set[i] = setToCopy.set[i].deepCopy();
        }
    }

    private HNSetWritable(HNSubSetWritable[] newSet, int newSize) {
        // TODO Auto-generated constructor stub
        set = newSet;//new HNSubSet[newSet.length];
        size = newSize;

        //		for (int i = 0; i < size; i++) {
        //			set[i] = newSet[i].deepCopy();
        //		}
    }

    /**
     * Retrieves the number of subsets contained in this <code>HNSetWritable</code>
     * object
     *
     * @return size of this <code>HNSetWritable</code> object
     */
    public final int size() {
        return size;
    }

    /**
     * Retrieves the <code> HNSubSet</code> object at a given position in this
     * <code> HNSetWritable</code> object
     *
     * @param index
     *            subset's position
     * @return <code>HNSubSet</code> object of at this position
     */
    public final HNSubSetWritable get(int index) {
        return set[index];
    }

    /**
     * Creates a deep copy of this <code> HNSetWritable</code> object
     *
     * @return a new <code>HNSetWritable</code> object with the same contents of the
     *         this <code>HNSetWritable</code> object
     */
    public final HNSetWritable deepCopy() {
        return new HNSetWritable(this);
    }

    public HNSetWritable deepCopy(Map<Integer, Integer> oldNewIndexMap) {
        // TODO Auto-generated method stub

        HNSubSetWritable[] newSet = new HNSubSetWritable[set.length];
        int newSize = size;

        for (int i = 0; i < size; i++) {
            newSet[i] = set[i].deepCopy(oldNewIndexMap);
        }

        return new HNSetWritable(newSet, newSize);
    }

    /**
     * Adds a given subset to this <code>HNSetWritable</code> object
     *
     * @param subset
     *            subset to be added to this <code>HNSetWritable</code> object
     * @throws <code>RuntimeException</code> if subset to add is
     *         <code>null</code>
     */
    public void add(HNSubSetWritable subset) {
        if (subset == null) {
            throw new RuntimeException("don't add null, I don't like that!");
        }

        // do binary search to find position of new element
        int pos = binarySearch(subset);

        if (pos < 0) {
            pos = (-pos - 1);

            // increase capacity if needed
            if (size == set.length) {
                HNSubSetWritable[] newSet = new HNSubSetWritable[set.length * 2];

                System.arraycopy(set, 0, newSet, 0, set.length);
                set = newSet;
            }

            // make space for the new element ...
            System.arraycopy(set, pos, set, pos + 1, size - pos);

            // ... and insert it
            set[pos] = subset;
            size++;
        }
    }

    /**
     * Adds all the subsets in a given <code> HNSetWritable</code> object to this
     * <code> HNSetWritable</code> object
     *
     * @param setToInclude
     *            set contains the subsets to add
     */
    public void addAll(HNSetWritable setToInclude) {
        if (set != null) {

            for (int i = 0; i < setToInclude.size(); i++) {
                HNSubSetWritable subset = setToInclude.get(i);
                // do binary search to find position of new element
                int pos = binarySearch(subset);

                if (pos < 0) {
                    pos = (-pos - 1);

                    // increase capacity if needed
                    if (size == set.length) {
                        HNSubSetWritable[] newSet = new HNSubSetWritable[set.length * 2];

                        System.arraycopy(set, 0, newSet, 0, set.length);
                        set = newSet;
                    }

                    // make space for the new element ...
                    System.arraycopy(set, pos, set, pos + 1, size - pos);

                    // ... and insert it
                    set[pos] = subset;
                    size++;
                }
            }
        }
    }

    /**
     * Checks if a certain subset is already contained in this
     * <code> HNSetWritable</code> object
     *
     * @param subset
     *            subset that may be contained in this <code> HNSetWritable</code>
     *            object
     * @return <code>true</code> if this <code> HNSetWritable</code> object contains the
     *         given <code>subset</code>, <code>false</code> otherwise.
     */
    public boolean contains(HNSubSetWritable subset) {
        return binarySearch(subset) >= 0;
    }

    /**
     * Removes a given subset of this <code> HNSetWritable</code> object
     *
     * @param subset
     *            subset to be removed from this <code>HNSetWritable</code> object
     */
    public void remove(HNSubSetWritable subset) {
        int pos = binarySearch(subset);

        if (pos >= 0) {
            System.arraycopy(set, pos + 1, set, pos, size - pos - 1);
            size--;
        }
    }

    /**
     * Removes from this <code> HNSetWritable</code> object all the subsets that are
     * contained in a given <code> HNSetWritable</code> object
     *
     * @param toRemove
     *            set with subsets to be removed from this <code>HNSetWritable</code>
     *            object
     */
    public void removeAll(HNSetWritable toRemove) {
        for (int i = 0; i < toRemove.size; i++) {
            remove(toRemove.get(i));
        }
    }

    /**
     * Retrieves the hash code of this <code> HNSetWritable</code> object
     *
     * @return hash code of this <code> HNSetWritable</code> object
     */
    @Override
    public int hashCode() {

        int hash = 0;
        for (int i = 0; i < size; i++) {
            hash += (get(i).hashCode() * 31 ^ (size - i + 1));

        }

        return hash;
    }

    /**
     * Compares if the given <code>HNSetWritable</code> object contains the same values
     * of this <code>HNSetWritable</code> object
     *
     * @param o <code>true</code> if the two <code>HNSetWritable</code> objects are the
     *        same, <code>false</code> otherwise
     */
    @Override
    public boolean equals(Object o) {
        HNSetWritable otherSet = (HNSetWritable) o;

        if ((otherSet == null) || (otherSet.size != size)) {
            return false;
        }

        for (int i = 0; i < size; i++) {
            if (!set[i].equals(otherSet.set[i])) {
                return false;
            }
        }
        return true;
    }

    // copied from the standard Arrays class, and adapted to our case:
    private int binarySearch(HNSubSetWritable key) {
        int low = 0;
        int high = size - 1; // search only the added elements, not the extra capacity elements

        while (low <= high) {
            int mid = (low + high) >> 1;
            HNSubSetWritable midVal = set[mid];
            int cmp = midVal.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }

    /**
     * Builds a string representation of this <code> HNSetWritable</code> object
     *
     * @return string representing this <code> HNSetWritable</code> object
     */
    @Override
    public String toString() {
        StringBuffer str = new StringBuffer("[");
        int i;

        for (i = 0; i < size; i++) {
            str.append(set[i].toString()).append(",");
        }
        if (i > 0) {
            str.deleteCharAt(str.length() - 1);
        }
        str.append("]");

        return str.toString();
    }

    /**
     * Calculates the union set of all the subsets contained in the given
     * <code> HNSetWritable</code> object
     *
     * @param set
     *            set contains all the subset to be united
     * @return union set of all the subsets in the given <code>set</code>
     */
    public static final HNSubSetWritable getUnionSet(HNSetWritable set) {
        HNSubSetWritable unionSet = new HNSubSetWritable();
        HNSubSetWritable subset = null;

        for (int i = 0; i < set.size(); i++) {
            subset = set.get(i);
            for (int j = 0; j < subset.size(); j++) {
                unionSet.add(subset.get(j));
            }

        }

        return unionSet;
    }

    /**
     * Removes a given element from all the subsets contained in the provided
     * <code> HNSetWritable</code> object
     *
     * @param set
     *            provided subset
     * @param element
     *            value to be removed from subsets
     * @return set where all the subsets do not contain <code>element</code>
     */
    public static HNSetWritable removeElementFromSubsets(HNSetWritable set, int element) {

        HNSetWritable returnSet = new HNSetWritable();
        HNSubSetWritable subset = null;

        for (int iSet = 0; iSet < set.size(); iSet++) {
            subset = set.get(iSet);
            subset.remove(element);
            if (subset.size() > 0) {
                returnSet.add(subset);
            }
        }

        return returnSet;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        HNSubSetArrayWritable arrayWritable = new HNSubSetArrayWritable();
        arrayWritable.set(set);
        arrayWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        HNSubSetArrayWritable arrayWritable = new HNSubSetArrayWritable();
        arrayWritable.readFields(in);
        set = (HNSubSetWritable[]) arrayWritable.get();
    }
}
