package org.zoodb.jdo.internal;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class is an implementation of the Set interface. To check object 
 * equality, it uses '==' instead of 'equals()'. 
 * 
 * @param <E>
 * @author Tilmann Zaeschke
 */
final public class ObjectIdentitySet<E> implements Set<E>
{
    ObjectIdentitySetEntry<E>[] table;
    int count;
    int threshold;

    static final float LOAD_FACTOR = 0.75f;
    static final int   INITIAL_CAPACITY = 127;

    /**
     * Create a new ObjectIdentitySet.
     */
    public ObjectIdentitySet () {
        this (INITIAL_CAPACITY);
    }

    public ObjectIdentitySet (int initial_capacity) {
        if (initial_capacity > INITIAL_CAPACITY) {
            //First get a good number: (2^n)-1 > initial_capacity
            //Ideally, n would be a prime, but that's getting too 
            //difficult here.
            int p = 1;
            while (initial_capacity > 1) {
                initial_capacity = initial_capacity >> 2;
                p++;
            }
            if (p%2 == 0) {
                p++;
            }
            initial_capacity = (int) Math.pow(2, p) - 1;
        } else {
            //Go for minimal feasible size (expecting resizing)
            initial_capacity = INITIAL_CAPACITY;
        }

        int initial_size = (int) (initial_capacity / LOAD_FACTOR);
        table            = new ObjectIdentitySetEntry [initial_size];
        count            = 0;
        threshold        = initial_capacity;
    }

    /**
     * @param object 
     * @return Returns <tt>true</tt> if the object was added, otherwise 
     * <tt>false</tt>.
     * @see java.util.Set#add(java.lang.Object)
     */
    public final boolean add (E object) {
        int hash  = System.identityHashCode (object);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> head = table [index];
        for (ObjectIdentitySetEntry<E> e = head; e != null; e = e.next) {
            if (object == e.object)
                return false;
        }
        table [index] = new ObjectIdentitySetEntry<E> (object, head);

        count++;
        if (count >= threshold)
            rehash (count * 2 + 1);
        return true;
    }

    final void add (E[] objects) {
        for (E obj: objects) {
            add (obj);
        }
    }

    /**
     * @see java.util.Set#contains(java.lang.Object)
     */
    public final boolean contains (Object object) {
        int hash  = System.identityHashCode (object);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> e;
        for (e = table [index]; e != null; e = e.next) {
            if (object == e.object)
                return true;
        }
        return false;
    }

    final void rehash (int new_length) {
        ObjectIdentitySetEntry<E>[] old_table = table;
        int old_length = old_table.length;
        //int new_length = old_length * 2 + 1;
        ObjectIdentitySetEntry<E>[] new_table =
            new ObjectIdentitySetEntry [new_length];

        for (int i = 0; i < old_length; i++) {
            ObjectIdentitySetEntry<E> next;
            ObjectIdentitySetEntry<E> e;
            for (e = old_table [i]; e != null; e = next) {
                int hash      = System.identityHashCode (e.object);
                int new_index = ((hash & 0x7FFFFFFF) % new_length);
                next = e.next;
                e.next = new_table [new_index];
                new_table [new_index] = e;
            }
        }

        table     = new_table;
        threshold = (int) (new_length * LOAD_FACTOR);
    }

    /**
     * @see java.util.Set#size()
     */
    public final int size() {
        return count;
    }

    /**
     * @see java.util.Set#clear()
     */
    public final void clear() {
        threshold = INITIAL_CAPACITY;
        count = 0;
        table = new ObjectIdentitySetEntry [(int) (threshold / LOAD_FACTOR)];
    }

    /**
     * @see java.util.Set#isEmpty()
     */
    public final boolean isEmpty() {
        return count == 0;
    }

    /**
     * @see java.util.Set#toArray()
     */
    public final Object[] toArray() {
        Object[] a = new Object[count];
        int j = 0;
        for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
            a[j++] = i.next();
        }
        return a;
    }

    /**
     * @see java.util.Set#remove(java.lang.Object)
     */
    public final boolean remove(Object obj) {
        int hash  = System.identityHashCode (obj);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> e;
        ObjectIdentitySetEntry<E> prev = null;
        for (e = table [index]; e != null; e = e.next) {
            if (obj == e.object) {
                //delete it
                if (prev == null) {
                    //first element
                    table [index] = e.next;
                } else {
                    prev.next = e.next;
                }

                count--;
                // rehash if < 25% filled and size > 127
                if (count < threshold*(1-LOAD_FACTOR) 
                        && count > INITIAL_CAPACITY)
                    rehash (count / 2 + 1);
                return true;
            }
            prev = e;
        }
        return false;
    }

    /**
     * @see java.util.Set#addAll(java.util.Collection)
     */
    public final boolean addAll(Collection<? extends E> c) {
    	boolean ret = false;
        for (E o: c) {
        	ret |= add(o);
        }
        return ret;
    }

    /**
     * Add all elements of in the enumeration
     * @param en
     */
    public final void addAll(Enumeration<E> en) {
        while (en.hasMoreElements()) {
            add(en.nextElement());
        }
    }

    /**
     * @see java.util.Set#containsAll(java.util.Collection)
     */
    public final boolean containsAll(Collection c) {
        if (c == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.util.Set#removeAll(java.util.Collection)
     */
    public final boolean removeAll(Collection c) {
        if (c == null) {
            throw new NullPointerException();
        }
        boolean hasChanged = false;
        for (Object o: c) {
            if (remove(o)) hasChanged = true;
        }
        return hasChanged;
    }

    /**
     * @see java.util.Set#retainAll(java.util.Collection)
     */
    public final boolean retainAll(Collection c) {
        if (c == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.util.Set#iterator()
     */
    public final Iterator<E> iterator() {
        return new ObjectIdentitySetIterator<E>(table);
    }

    /**
     * @param a 
     * @param <T> 
     * @return Array representation of this container
     * @see java.util.Set#toArray(Object[])
     */
    public final <T> T[] toArray(T[] a) {
        if (a == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.lang.Object#toString()
     */
    public final String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("ObjectIdentitySet(" + count + "): ");
        for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
            buf.append(i.next() + "; ");
        }
        return buf.toString();
    }
}

final class ObjectIdentitySetEntry<T>
{
    T object;
    ObjectIdentitySetEntry<T> next;

    ObjectIdentitySetEntry (T object1, ObjectIdentitySetEntry<T> next1) {
        object = object1;
        next   = next1;
    }
}

final class ObjectIdentitySetIterator<T> implements Iterator<T> {

    private int _pos = 0;
    private ObjectIdentitySetEntry<T> _next; 
    private ObjectIdentitySetEntry<T>[] _table;

    ObjectIdentitySetIterator(ObjectIdentitySetEntry<T>[] table) {
        _table = table;
        _next = null;
    }

    /**
     * @see java.util.Iterator#remove()
     */
    public final void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.util.Iterator#hasNext()
     */
    public final boolean hasNext() {
        if (_next == null) {
            for( ; _pos < _table.length; _pos++) {
                if (_table[_pos] != null) {
                    _next = _table[_pos];
                    _pos++;
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * @see java.util.Iterator#next()
     */
    public final T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        ObjectIdentitySetEntry<T> n = _next;
        _next = _next.next;
        return n.object;
    }
}

