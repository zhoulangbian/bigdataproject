package com.javacodegeeks.examples.wordcount;

import java.io.Serializable;

public class Pair<K extends Comparable<K>, V> implements Comparable<Pair<K, V>>,Serializable {

    private static final long serialVersionUID = 8318814216717367973L;

    private K k;
    private V v;

    Pair(K k, V v){
        this.k = k;
        this.v = v;
    }

    public K getT() {
        return k;
    }

    public V getValue() {
        return v;
    }
    
    public void setValue(V v) {
        this.v = v;
    }

    @Override
    public int compareTo(Pair<K, V> o) {
        return getT().compareTo(o.getT());
    }
}
