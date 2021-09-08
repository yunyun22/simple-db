package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * TupleDesc describes the schema of a tuple.
 *
 * @author jinqwang
 */
public class TupleDesc implements Serializable {

    /**
     * 元组描述的数据类型
     */
    private final TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return new Iter();
    }

    private final class Iter implements Iterator<TDItem> {
        int i = 0;

        @Override
        public boolean hasNext() {
            return i < tdItems.length;
        }

        @Override
        public TDItem next() {
            return tdItems[i++];
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("");
        }
        if (fieldAr == null || fieldAr.length == 0) {
            throw new IllegalArgumentException("");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("");
        }

        TDItem[] tdItems = new TDItem[typeAr.length];
        int i = 0;
        for (Type type : typeAr) {
            tdItems[i] = new TDItem(type, fieldAr[i]);
            i++;
        }
        this.tdItems = tdItems;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("");
        }
        TDItem[] tdItems = new TDItem[typeAr.length];
        int i = 0;
        for (Type type : typeAr) {
            tdItems[i++] = new TDItem(type, "unnamed");
        }
        this.tdItems = tdItems;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        if (i < 0 || i > tdItems.length - 1) {
            throw new NoSuchElementException();
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {

        if (i < 0 || i > tdItems.length - 1) {
            throw new NoSuchElementException();
        }

        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        for (int i = 0; i < tdItems.length; i++) {
            TDItem tdItem = tdItems[i];
            if (Objects.equals(tdItem.fieldName, name)) {
                return i;
            }
        }
        throw new NoSuchElementException(name == null ? "null" : name + "is not a valid field name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total = 0;
        for (TDItem item : tdItems) {
            total += item.fieldType.getLen();
        }
        return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newLength = td1.tdItems.length + td2.tdItems.length;
        Type[] types = new Type[newLength];
        String[] names = new String[newLength];
        Iterator<TDItem> ite1 = td1.iterator();
        int i = 0;
        while (ite1.hasNext()) {
            TDItem next = ite1.next();
            types[i] = next.fieldType;
            names[i] = next.fieldName;
            i++;
        }

        Iterator<TDItem> it2 = td2.iterator();
        while (it2.hasNext()) {
            TDItem next = it2.next();
            types[i] = next.fieldType;
            names[i] = next.fieldName;
            i++;
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            TupleDesc tupleDesc = (TupleDesc) o;
            if (this.tdItems.length == tupleDesc.tdItems.length) {
                for (int i = 0; i < tdItems.length; i++) {
                    if (this.tdItems[i].fieldType != tupleDesc.tdItems[i].fieldType) {
                        return false;
                    }
                    if (!Objects.equals(this.tdItems[i].fieldName, tupleDesc.tdItems[i].fieldName)) {
                        return false;
                    }
                }
                return true;
            }

        }
        return false;
    }

    @Override
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
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tdItems.length; i++) {
            sb.append(tdItems[i].fieldType.toString())
                    .append("[")
                    .append(i)
                    .append("](")
                    .append(tdItems[i].fieldName)
                    .append("[")
                    .append(i)
                    .append("]")
                    .append(")");
        }
        return sb.toString();
    }
}
