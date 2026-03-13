package it.unitn.models;
 
/**
 * Immutable value+version pair stored at each replica.
 * Version is strictly increasing; reads return the highest version seen across R replies.
 */
public record StorageData(String value, int version) implements Comparable<StorageData> {
    @Override
    public int compareTo(StorageData storageData) {
        return Integer.compare(this.version, storageData.version);
    }
}
