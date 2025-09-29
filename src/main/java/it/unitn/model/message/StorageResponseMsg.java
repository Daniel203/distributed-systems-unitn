package it.unitn.model.message;

import it.unitn.model.StorageData;

import java.util.TreeMap;

public record StorageResponseMsg(TreeMap<Integer, StorageData> storage) {
}
