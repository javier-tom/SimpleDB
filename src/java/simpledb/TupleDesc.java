package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdList.iterator();
    }

    private static final long serialVersionUID = 1L;

    // added fields
    Type[] typeArr;
    String[] fieldArr;
    List<TDItem> tdList = new ArrayList<>();

    // Helper Methods

    /**
     * Creates a TDItem list for arguments passed into the constructor
     * @param typeAr
     *          array specifying the number of and types of fields in this
     *          TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *          array specifying the names of the fields. Note that names may
     *          be null.
     */
    private void createTDList(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            tdList.add(tdItem);
        }
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.typeArr = typeAr;
        this.fieldArr = fieldAr;
        createTDList(typeAr, fieldAr);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (numFields() <= i) {
            throw new NoSuchElementException();
        }
        return this.tdList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (numFields() <= i) {
            throw new NoSuchElementException();
        }
        return this.tdList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) throw new NoSuchElementException();

        // iterate through the fieldArr until we find a match or throw exception
        for (int i = 0; i < numFields(); i++) {
            if (name.equals(tdList.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // iterate through our typeArr and call Type getlen() method
        int totalBytesNeeded = 0;
        Iterator<TDItem> iterator = this.iterator();
        while (iterator.hasNext()) {
            totalBytesNeeded += iterator.next().fieldType.getLen();
        }
        return totalBytesNeeded;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        // create a new Type[] and String[] of size td1.numfields() + td2.numfields()
        Type[] mergedTypes = new Type[td1.numFields() + td2.numFields()];
        String[] mergedFields = new String[td1.numFields() + td2.numFields()];
        int index = 0;
        for (; index < td1.numFields(); index++) {
            mergedTypes[index] = td1.getFieldType(index);
            mergedFields[index] = td1.getFieldName(index);
        }
        for (int i = 0; i + index < mergedFields.length; i++) {
            mergedTypes[i + index] = td2.getFieldType(i);
            mergedFields[i + index] = td2.getFieldName(i);
        }
        return new TupleDesc(mergedTypes, mergedFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // check to see if o is of type TupleDesc
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;

        // check to see of num fields is of the same size
        if (this.numFields() != td.numFields()) {
            return false;
        }

        for (int i = 0; i < numFields(); i++) {
            if (!(this.getFieldType(i).equals(td.getFieldType(i)))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        Iterator<TDItem> iterator = iterator();
        while (iterator.hasNext()) {
            TDItem tdItem = iterator.next();
            sb.append(tdItem.fieldType + "(" + tdItem.fieldName + "),");
        }

        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
