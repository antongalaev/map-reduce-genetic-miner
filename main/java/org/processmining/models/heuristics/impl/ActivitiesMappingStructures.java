package org.processmining.models.heuristics.impl;

import com.galaev.genminer.mapred.writables.arrays.XEventClassArrayWritable;
import org.apache.hadoop.io.Writable;
import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Aids the creation of <code>HeuristicsNets</code> objects. In short, this
 * class guarantees that indexes assigned to activities in
 * <code>HeuristicsNet</code> objects are always the same whenever (i)
 * <code>XEventClasses</code> objects have the same contents and (ii) the same
 * number of activities must be created for a given <code>XEventClassWritable</code>
 * object.
 *
 * @author Ana Karla Alves de Medeiros
 * @author Anton Galaev
 *
 */
public class ActivitiesMappingStructures implements Writable {

    private static final String VISUALIZATION_SEPARATOR = ",";

    private XEventClasses xEventClasses; // allow to map the events to the internal indexes used for the activities

    private XEventClass[] activitiesMapping; //Maps activities to events

    private Map<XEventClass, HNSubSet> reverseActivitiesMapping; //Maps events to activities

    /**
     * Default constructor for Hadoop serialization purposes.
     */
    public ActivitiesMappingStructures() {
        xEventClasses = new XEventClasses();
        activitiesMapping = new XEventClass[0];
        reverseActivitiesMapping = new HashMap<>();
    }

    /**
     * Builds an <code>ActivitiesMappingStructuresWritable</code> object that contains a
     * single activity for each event in the provided <code>XEventClassWritable</code>
     * object.
     *
     * @param events
     *            event classes to which the activities mapping should be
     *            created.
     */
    public ActivitiesMappingStructures(XEventClasses events) {
        //building the mapping to indicate that every XEventClassWritable
        //has exactly one appearance in the HeuristicsNet
        Map<XEventClass, Integer> numberOfDuplicatesPerEvent = new HashMap<>();
        for (XEventClass XEventClass : events.getClasses()) {
            Integer numberDuplicates = 1; //It is "1" because there are no duplicates!
            numberOfDuplicatesPerEvent.put(XEventClass, numberDuplicates);
        }

        //calling the method that creates the necessary internal structures
        initializeVariables(events, numberOfDuplicatesPerEvent);

    }

    /**
     * Builds an <code>ActivitiesMappingStructuresWritable</code> that may contain
     * duplicate activities for a given <code>XEventClassWritable</code>. The exact
     * number of activities by <code>XEventClassWritable</code> event is provided in a
     * mapping structure from <code>XEventClassWritable</code> to <code>Integer</code>.
     * <p>
     * <b>Note:</b> Every event should have at least one activity assigned to
     * it.
     *
     * @param events
     *            event classes to include in this
     *            <code>ActivitiesMappingStructuresWritable</code> object.
     * @param numberOfActivitiesPerEvent
     *            mapping specifying the number of activities that should be
     *            created in this <code>ActivitiesMappingStructuresWritable</code> based
     *            on the provided <code>XEventClassWritable</code> object.
     * @throws <code>IllegalArgumentException</code> if (i) the
     *         <code>numberOfActivitiesPerEvent</code> does not contain an entry
     *         for a given <code>XEventClassWritable</code> object in events or (ii) the
     *         <code>numberOfActivitiesPerEvent</code> returns an
     *         <code>Integer</code> with value inferior to one 1 for this
     *         <code>XEventClassWritable</code> object.
     */

    public ActivitiesMappingStructures(XEventClasses events, Map<XEventClass, Integer> numberOfActivitiesPerEvent)
            throws IllegalArgumentException {
        //first, quick check to see if all activities have a number of activities
        if (events.size() != numberOfActivitiesPerEvent.size()) {
            throw new IllegalArgumentException("Missing number of activities for some events!");
        }
        //now, check if all numbers are bigger than zero ("0")
        for (XEventClass key : numberOfActivitiesPerEvent.keySet()) {
            if (numberOfActivitiesPerEvent.get(key) < 1) {
                throw new IllegalArgumentException(
                        "Some events have a number of activities that is inferior to one (\"1\")!");
            }
        }

        initializeVariables(events, numberOfActivitiesPerEvent);
    }

    /**
     * Initializes the internal structures of the HeuristicsNet. The
     * initialization is based on the provided XEventClasses and the mapping
     * determining the number of activities to be created to each event.
     *
     * @param events
     *            Set of XEventClasses in the log.
     * @param numberOfActivitiesPerEvent
     *            Mapping indicating how many activities to create to each
     *            element in the input parameter events.
     */
    private void initializeVariables(XEventClasses events, Map<XEventClass, Integer> numberOfActivitiesPerEvent) {

        xEventClasses = events;

        //creating the activities mapping
        activitiesMapping = createArrayWithCorrectNumberActivities(events, numberOfActivitiesPerEvent);
        Arrays.sort(activitiesMapping, new XEventClassComparator());

        //creating the reverse activities mapping
        reverseActivitiesMapping = buildReverseActivitiesMapping(activitiesMapping);

    }

    /**
     * Creates an array with the correct number of activities per event class
     *
     * @param events
     *            event classes
     * @param numberOfActivitiesPerEvent
     *            mapping specifying the number of activities per event.
     * @return an array of XEventClassWritable objects.
     */
    private static XEventClass[] createArrayWithCorrectNumberActivities(XEventClasses events,
                                                                                Map<XEventClass, Integer> numberOfActivitiesPerEvent) {
        ArrayList<XEventClass> arrayList = new ArrayList<>();

        Iterator<XEventClass> iteratorOverEventClasses = events.getClasses().iterator();
        while (iteratorOverEventClasses.hasNext()) {
            XEventClass currentEvent = iteratorOverEventClasses.next();
            int numberOfActivitiesToCreate = numberOfActivitiesPerEvent.get(currentEvent);
            for (int i = 0; i < numberOfActivitiesToCreate; i++) {
                arrayList.add(currentEvent);
            }
        }

        return arrayList.toArray(new XEventClass[arrayList.size()]);

    }

    /**
     * Builds a mapping from XEventClassWritable elements to activities in the
     * ActivitiesMapping. This mapping is a reverse mapping of the mapping from
     * activities to XEventClassWritable.
     *
     * @param activitiesMapping
     *            original mapping.
     * @return the reverse mapping of the original mapping.
     */
    private static Map<XEventClass, HNSubSet> buildReverseActivitiesMapping(XEventClass[] activitiesMapping) {

        Map<XEventClass, HNSubSet> reverseActivitiesMapping = new HashMap<>();

        for (int index = 0; index < activitiesMapping.length; index++) {
            XEventClass currentEvent = activitiesMapping[index];
            if (!reverseActivitiesMapping.containsKey(currentEvent)) {
                reverseActivitiesMapping.put(currentEvent, new HNSubSet());
            }
            HNSubSet activitiesMappingToCurrentEvent = reverseActivitiesMapping.get(currentEvent);
            activitiesMappingToCurrentEvent.add(index);
            reverseActivitiesMapping.put(currentEvent, activitiesMappingToCurrentEvent);
        }

        return reverseActivitiesMapping;

    }

    /**
     * Retrieves the current <code>XEventClasses</code> object used by this
     * <code>ActivitiesMappingStructuresWritable</code> object.
     *
     * @return XEventClasses event classes used by this
     *         <code>ActivitiesMappingStructuresWritable</code> object.
     */
    public XEventClasses getXEventClasses() {
        return xEventClasses;
    }

    /**
     * Retrieves the current mapping from activities to <code>XEventClassWritable</code>
     * events.
     *
     * @return array containing the current mapping.
     */
    public XEventClass[] getActivitiesMapping() {
        return activitiesMapping;
    }

    /**
     * Retrieves the current mapping from <code>XEventClassWritable</code> events to
     * activities.
     *
     * @return Map mapping from <code>XEventClassWritable</code> events to activities.
     */
    public Map<XEventClass, HNSubSet> getReverseActivitiesMapping() {
        return reverseActivitiesMapping;
    }

    /**
     * Creates the string representation of this
     * <code>ActivitiesMappingStructure</code> object.
     *
     * @return String string representation of this
     *         <code>ActivitiesMappingStructure</code> object.
     */
    @Override
    public String toString() {

        StringBuffer stringRepresentation = new StringBuffer();

        stringRepresentation.append("Activities Mapping = [");
        for (int i = 0; i < activitiesMapping.length; i++) {
            stringRepresentation.append(i).append("=").append(activitiesMapping[i]).append(VISUALIZATION_SEPARATOR);
        }
        stringRepresentation.delete(stringRepresentation.lastIndexOf(VISUALIZATION_SEPARATOR), stringRepresentation
                .length());
        stringRepresentation.append("]");
        stringRepresentation.append("\n");

        stringRepresentation.append("Reverse Activities Mapping = [");
        for (XEventClass eventClass : reverseActivitiesMapping.keySet()) {
            stringRepresentation.append(eventClass).append("=").append(reverseActivitiesMapping.get(eventClass))
                    .append(VISUALIZATION_SEPARATOR);
        }

        stringRepresentation.delete(stringRepresentation.lastIndexOf(VISUALIZATION_SEPARATOR), stringRepresentation
                .length());
        stringRepresentation.append("]");

        return stringRepresentation.toString();

    }

    /**
     * Compares another <code>ActivitiesMappingStructure</code> object with this
     * <code>ActivitiesMappingStructure</code> object.
     *
     * @param o
     *            other <code>ActivitiesMappingStructure</code> object
     * @return boolean <code>true</code> if the two objects are the same,
     *         <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object o) {

        if (!(o instanceof ActivitiesMappingStructures) || (o == null)) {
            return false;
        } else {
            ActivitiesMappingStructures other = (ActivitiesMappingStructures) o;
            if (activitiesMapping.length != other.getActivitiesMapping().length) {
                return false;
            } else {
                for (int i = 0; i < activitiesMapping.length; i++) {
                    if (!activitiesMapping[i].equals(other.getActivitiesMapping()[i])) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * Calculates the hash code value of this
     * <code>ActivitiesMappingStructure</code> object. The hash code value is
     * based on the string representation of this object.
     *
     * @return the hash code value for this object
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        xEventClasses.write(out);
        // write activities mapping
        XEventClassArrayWritable arrayWritable = new XEventClassArrayWritable();
        arrayWritable.set(activitiesMapping);
        arrayWritable.write(out);
        // write reverse activities mapping
        out.writeInt(reverseActivitiesMapping.size());
        for (XEventClass key : reverseActivitiesMapping.keySet()) {
            key.write(out);
            HNSubSet value = reverseActivitiesMapping.get(key);
            value.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        xEventClasses.readFields(in);
        // read activities mapping
        XEventClassArrayWritable arrayWritable = new XEventClassArrayWritable();
        arrayWritable.readFields(in);
        Writable[] writables =  arrayWritable.get();
        activitiesMapping = new XEventClass[writables.length];
        for (int i = 0; i < activitiesMapping.length; i++) {
            activitiesMapping[i] = (XEventClass) writables[i];
        }
        // read reverse activities mapping
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            XEventClass key = new XEventClass();
            key.readFields(in);
            HNSubSet value = new HNSubSet();
            value.readFields(in);
            reverseActivitiesMapping.put(key, value);
        }
    }
}

/**
 * This class compares two <code>XEventClassWritable</code> objects. The ordering is
 * based on the strings that represent the two <code>XEventClassWritable</code> objects.
 *
 * @author Ana Karla Alves de Medeiros
 *
 */
class XEventClassComparator implements Comparator<XEventClass> {
    /**
     * Compares two <code>XEventClassWritable</code> objects <code>o1</code> and
     * <code>o2</code>.
     *
     * @param o1
     *            first object to be used in the comparison
     * @param o2
     *            second object to be used in the comparison
     * @return the value 0 if the string representation of <code>o1</code> is
     *         equal to the one for <code>o2</code>; a value less than 0 if the
     *         string for <code>o1</code> is lexicographically less than the
     *         string for <code>o2</code>; and a value greater than 0 if the
     *         string for <code>o1</code> is lexicographically greater than the
     *         string for <code>o2</code>.
     */
    public int compare(XEventClass o1, XEventClass o2) {
        return o1.getId().compareTo(o2.getId());
    }

}