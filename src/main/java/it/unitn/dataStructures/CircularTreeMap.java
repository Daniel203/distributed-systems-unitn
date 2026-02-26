package it.unitn.dataStructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CircularTreeMap<K, V> {
    private TreeMap<K, V> map;

    public CircularTreeMap() {
        this.map = new TreeMap<>();
    }

    public void put(K key, V value) {
        this.map.put(key, value);
    }

    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    public void putAll(CircularTreeMap<K, V> map) {
        this.map.putAll(map.map);
    }

    public V get(K key) {
        return this.map.get(key);
    }

    public V remove(K key) {
        return this.map.remove(key);
    }

    public boolean containsKey(K key) {
        return this.map.containsKey(key);
    }

    public int size() {
        return this.map.size();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }


    public V getNext(K key) {
        K nextKey = this.map.higherKey(key);
        if (nextKey == null) {
            nextKey = this.map.firstKey(); // Wrap around
        }
        return this.map.get(nextKey);
    }

    public V getNthNext(K key, int n) {
        K currentKey = key;
        for (int i = 0; i < n; i++) {
            currentKey = this.map.higherKey(currentKey);
            if (currentKey == null) {
                currentKey = this.map.firstKey(); // Wrap around
            }
        }
        return this.map.get(currentKey);
    }

    public V getPrevious(K key) {
        K prevKey = this.map.lowerKey(key);
        if (prevKey == null) {
            prevKey = this.map.lastKey(); // Wrap around
        }
        return this.map.get(prevKey);
    }

    public V getNthPrevious(K key, int n) {
        K currentKey = key;
        for (int i = 0; i < n; i++) {
            currentKey = this.map.lowerKey(currentKey);
            if (currentKey == null) {
                currentKey = this.map.lastKey(); // Wrap around
            }
        }
        return this.map.get(currentKey);
    }

    public K nextKey(K key) {
        K next = this.map.higherKey(key);
        return next != null ? next : this.map.firstKey();
    }

    public K nthNextKey(K key, int n) {
        K currentKey = key;
        for (int i = 0; i < n; i++) {
            currentKey = this.map.higherKey(currentKey);
            if (currentKey == null) {
                currentKey = this.map.firstKey();
            }
        }
        return currentKey;
    }

    public K previousKey(K key) {
        K prev = this.map.lowerKey(key);
        return prev != null ? prev : this.map.lastKey();
    }

    public K nthPreviousKey(K key, int n) {
        K currentKey = key;
        for (int i = 0; i < n; i++) {
            currentKey = this.map.lowerKey(currentKey);
            if (currentKey == null) {
                currentKey = this.map.lastKey();
            }
        }
        return currentKey;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return this.map.entrySet();
    }

    /**
     * Returns the current internal map (read-only).
     */
    public Map<K, V> getMap() {
        return Map.copyOf(map);
    }


    /**
     * Finds the exact N nodes responsible for a given dataKey.
     */
    public List<K> getNResponsibleNodes(K dataKey, int n) {
        List<K> responsibleNodes = new ArrayList<>();
        if (this.map.isEmpty()) return responsibleNodes;

        // find the first node >= dataKey, then walks clockwise
        K currentNode = this.map.ceilingKey(dataKey);
        if (currentNode == null) {
            currentNode = this.map.firstKey(); // Wrap around
        }

        for (int i = 0; i < n; i++) {
            responsibleNodes.add(currentNode);
            currentNode = this.map.higherKey(currentNode);
            if (currentNode == null) {
                currentNode = this.map.firstKey(); // Wrap around
            }
        }
        return responsibleNodes;
    }

    /**
     * Checks if a specific nodeId is one of the N responsible nodes for a dataKey.
     */
    public boolean isResponsible(K nodeId, K dataKey, int n) {
        return getNResponsibleNodes(dataKey, n).contains(nodeId);
    }

    /**
     * Creates a copy of the current ring, excluding a specific node.
     * Perfect for calculating the network topology when a node leaves.
     */
    public CircularTreeMap<K, V> cloneWithout(K nodeIdToRemove) {
        CircularTreeMap<K, V> clone = new CircularTreeMap<>();
        clone.putAll(this.map);
        clone.remove(nodeIdToRemove);
        return clone;
    }
}
