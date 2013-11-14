package distsys.mapreduce;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/10/13
 */
public class Record<K, V> implements Serializable {

    private K key;
    private V value;

    /**
     * A generic object that is passed between stages of Map Reduce, containing the key
     * and value of each record read from KDFS and operated on by map() and reduce()
     *
     * @param key   Generic type of key
     * @param value Generic type of value
     */
    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

}

