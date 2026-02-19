package it.unitn.model;

public record StorageData(String value, int version) implements Comparable<StorageData> {
    @Override
    public int compareTo(StorageData storageData) {
        return Integer.compare(this.version, storageData.version);
    }
}
