package it.unitn.models;

public record StorageData(String value, int version) implements Comparable<StorageData> {
    @Override
    public int compareTo(StorageData storageData) {
        return Integer.compare(this.version, storageData.version);
    }
}
