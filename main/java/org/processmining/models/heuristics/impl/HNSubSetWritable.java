package org.processmining.models.heuristics.impl;

import com.galaev.mapreduce.geneticminer.writables.arrays.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Ordered set of <code>int</code> that is used to represent subsets in the
 * input and output sets of <code>HeuristicsNet</code> objects.
 *
 * @author Peter van den Brand and Ana Karla Alves de Medeiros
 *
 */
public class HNSubSetWritable  implements WritableComparable<HNSubSetWritable> {

    private int[] subset;
    private int size;
    private int hash;

    private static int[] hashValues;

    static {
        int num = 1000;

        hashValues = new int[num];
        for (int i = 0; i < num; i++) {
            hashValues[i] = i * ((int) Math.pow(31, i));
        }
    }

    /**
     * Constructs a <code>HNSubSetWritable</code> object
     */
    public HNSubSetWritable() {
        subset = new int[10];
        size = 0;
        hash = 0;
    }

    // this constructor is only used by deepCopy
    private HNSubSetWritable(HNSubSetWritable setToCopy) {
        subset = new int[setToCopy.subset.length];
        System.arraycopy(setToCopy.subset, 0, subset, 0, setToCopy.size);
        size = setToCopy.size;
        hash = setToCopy.hash;
    }

    private HNSubSetWritable(int[] newSubset, int newSize, int newHash) {
        // TODO Auto-generated constructor stub
        subset = newSubset;//new int[newSubset.length];
        //		System.arraycopy(newSubset, 0, subset, 0, newSize);
        size = newSize;
        hash = newHash;
    }

    /**
     * Retrieves the number of values contained in this <code> HNSubSetWritable</code>
     * object
     *
     * @return size of this <code> HNSubSetWritable</code> object
     */
    public final int size() {
        return size;
    }

    /**
     * Retrieves the value at a given position in this <code> HNSubSetWritable</code>
     * object
     *
     * @param index
     *            element's position
     * @return value of the element at this position
     */
    public final int get(int index) {
        return subset[index];
    }

    /**
     * Checks if a certain value is already contained in this
     * <code> HNSubSetWritable</code> object
     *
     * @param value
     *            number that may be contained in this <code> HNSubSetWritable</code>
     *            object
     * @return <code>true</code> if this <code> HNSubSetWritable</code> object contains
     *         the given <code>value</code>, <code>false</code> otherwise.
     */
    public final boolean contains(int value) {
        return binarySearch(value) >= 0;
    }

    /**
     * Creates a deep copy of this <code> HNSubSetWritable</code> object
     *
     * @return a new <code> HNSubSetWritable</code> object with the same contents of the
     *         this <code> HNSubSetWritable</code> object
     */
    public final HNSubSetWritable deepCopy() {
        return new HNSubSetWritable(this);
    }

    public HNSubSetWritable deepCopy(Map<Integer, Integer> oldNewIndexMap) {
        // TODO Auto-generated method stub

        //HNSubSetWritable subset = new HNSubSetWritable();
        int[] newSubset = new int[subset.length];
        //System.arraycopy(setToCopy.subset, 0, subset, 0, setToCopy.size);
        for (int i = 0; i < size; i++) {
            newSubset[i] = oldNewIndexMap.get(subset[i]);
        }
        int newSize = size;
        int newHash = hash;

        return new HNSubSetWritable(newSubset, newSize, newHash);

    }

    /**
     * Adds all the values in a given <code> HNSubSetWritable</code> object to this
     * <code> HNSubSetWritable</code> object
     *
     * @param toAdd
     *            subset contains the values to add
     */
    public void addAll(HNSubSetWritable toAdd) {
        for (int i = 0; i < toAdd.size; i++) {
            add(toAdd.get(i));
        }
    }

    /**
     * Adds a given value to this <code> HNSubSetWritable</code> object
     *
     * @param value
     *            integer to be added to this <code> HNSubSetWritable</code> object
     */
    public void add(int value) {

        // do binary search to find position of new element
        int pos = binarySearch(value);

        if (pos < 0) {
            pos = (-pos - 1);

            // increase capacity if needed
            if (size == subset.length) {
                int[] newSubset = new int[subset.length * 2];

                System.arraycopy(subset, 0, newSubset, 0, subset.length);
                subset = newSubset;
            }

            // make space for the new element ...
            System.arraycopy(subset, pos, subset, pos + 1, size - pos);

            // ... and insert it
            subset[pos] = value;
            size++;

            hash += hashValues[Math.abs(value % hashValues.length)];
        }
    }

    /**
     * Builds a string representation of this <code> HNSubSetWritable</code> object
     *
     * @return string representing this <code> HNSubSetWritable</code> object
     */
    @Override
    public String toString() {
        StringBuffer str = new StringBuffer("[");
        int i;

        for (i = 0; i < size; i++) {
            str.append(subset[i]).append(",");
        }
        if (i > 0) {
            str.deleteCharAt(str.length() - 1);
        }
        str.append("]");

        return str.toString();

    }

    /**
     * Removes a given value of this <code> HNSubSetWritable</code> object
     *
     * @param value
     *            integer to be removed from this <code> HNSubSetWritable</code> object
     */
    public void remove(int value) {
        int pos = binarySearch(value);

        if (pos >= 0) {
            System.arraycopy(subset, pos + 1, subset, pos, size - pos - 1);
            size--;

            hash -= hashValues[Math.abs(value % hashValues.length)];
        }
    }

    /**
     * Removes from this <code> HNSubSetWritable</code> object all the values that are
     * contained in a given <code> HNSubSetWritable</code> object
     *
     * @param toRemove
     *            subset with values to be removed from this
     *            <code>HNSubSetWritable</code> object
     */
    public void removeAll(HNSubSetWritable toRemove) {
        for (int i = 0; i < toRemove.size; i++) {
            remove(toRemove.get(i));
        }
    }

    /**
     * Retrieves the hash code of this <code> HNSubSetWritable</code> object
     *
     * @return hash code of this <code> HNSubSetWritable</code> object
     */
    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * Compares a given <code> HNSubSetWritable</code> object with this
     * <code> HNSubSetWritable</code> object. The comparison occurs in the following
     * way:
     * <ol>
     * <li>A non-null <code>HNSubSetWritable</code> object is always 'less than' a
     * <code>null</code> object</li>;
     * <li>A <code>HNSubSetWritable</code> object with smaller size is always 'less
     * than' a <code>HNSubSetWritable</code> object with bigger size</li>;
     * <li>If two objects are non-null and have the same size, than their
     * ordered list of values will be compared. The first set to have a bigger
     * element at a given position is the bigger one</li>.
     * </ol>
     *
     * @param set
     *            heuristics net subset to compare with this
     *            <code>HNSubSetWritable</code> object
     * @return <code>-1</code> if the other <code>HNSubSetWritable</code> object if
     *         smaller than this <code>HNSubSetWritable</code> object, <code>0</code> if
     *         both objects are the equal, and <code>1</code> otherwise.
     */
    public int compareTo(HNSubSetWritable set) {
        if (set == null) {
            return -1;
        }

        if (size != set.size) {
            return size < set.size ? -1 : 1;
        }

        for (int i = 0; i < size; i++) {
            if (subset[i] != set.subset[i]) {
                return subset[i] < set.subset[i] ? -1 : 1;
            }
        }
        return 0;
    }

    /**
     * Compares if the given <code>HNSubSetWritable</code> object contains the same
     * values of this <code>HNSubSetWritable</code> object
     *
     * @param o <code>true</code> if the two <code>HNSubSetWritable</code> objects are the
     *        same, <code>false</code> otherwise
     */
    @Override
    public boolean equals(Object o) {
        HNSubSetWritable set = (HNSubSetWritable) o;

        if ((set == null) || (set.size != size) || (set.hash != hash)) {
            return false;
        }

        // 'i' goes from size - 1 to 0 to gain a bit of speed
        for (int i = size - 1; i >= 0; i--) {
            if (subset[i] != set.subset[i]) {
                return false;
            }
        }
        return true;
    }

    // copied from the standard Arrays class, and adapted to our case:
    private int binarySearch(int key) {
        int low = 0;
        int high = size - 1; // search only the added elements, not the extra capacity elements

        while (low <= high) {
            int mid = (low + high) >> 1;
            int midVal = subset[mid];

            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(hash);
        IntWritable[] intWritables = new IntWritable[subset.length];
        for (int i = 0; i < subset.length; ++i) {
            intWritables[i] = new IntWritable(subset[i]);
        }
        IntArrayWritable arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritables);
        arrayWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        hash = in.readInt();
        IntArrayWritable arrayWritable = new IntArrayWritable();
        arrayWritable.readFields(in);
        IntWritable[] intWritables = (IntWritable[]) arrayWritable.get();
        subset = new int[intWritables.length];
        for (int i = 0; i < subset.length; ++i) {
            subset[i] = intWritables[i].get();
        }
    }
}
