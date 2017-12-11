package se.uu.ub.cora.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;

class CollectedDataOrganiser {
	private Map<String, DataGroup> collectedDataByDataDivider;
	private String recordType;
	private String key;
	private int repeatId;
	private String id;

	protected Map<String, DataGroup> structureCollectedDataForDisk(
			Map<String, Map<String, Map<String, List<StorageTermData>>>> terms) {
		collectedDataByDataDivider = new HashMap<>();
		repeatId = 0;
		for (Entry<String, Map<String, Map<String, List<StorageTermData>>>> entryRecordType : terms
				.entrySet()) {
			recordType = entryRecordType.getKey();
			loopKeysAndCreateStorageTerms(entryRecordType.getValue());
		}
		return collectedDataByDataDivider;
	}

	private void loopKeysAndCreateStorageTerms(Map<String, Map<String, List<StorageTermData>>> map) {
		for (Entry<String, Map<String, List<StorageTermData>>> mapForEntryKey : map.entrySet()) {
			key = mapForEntryKey.getKey();
			loopRecordIdsAndCreateStorageTerms(mapForEntryKey.getValue());
		}
	}

	private void loopRecordIdsAndCreateStorageTerms(Map<String, List<StorageTermData>> map) {
		for (Entry<String, List<StorageTermData>> idEntry : map.entrySet()) {
			id = idEntry.getKey();
			loopStorageTermDataAndCreateStorageTerms(idEntry);
		}
	}

	private void loopStorageTermDataAndCreateStorageTerms(Entry<String, List<StorageTermData>> idEntry) {
		for (StorageTermData storageTermData : idEntry.getValue()) {
			DataGroup storageTerm = createStorageTerm(storageTermData);
			addStorageTermToResult(storageTermData, storageTerm);
		}
	}

	private DataGroup createStorageTerm(StorageTermData storageTermData) {
		DataGroup storageTerm = DataGroup.withNameInData("storageTerm");
		storageTerm.setRepeatId(String.valueOf(repeatId));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("type", recordType));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("key", key));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("id", id));

		storageTerm.addChild(DataAtomic.withNameInDataAndValue("value", storageTermData.value));
		storageTerm
				.addChild(DataAtomic.withNameInDataAndValue("dataDivider", storageTermData.dataDivider));
		repeatId++;
		return storageTerm;
	}

	private void addStorageTermToResult(StorageTermData storageTermData, DataGroup storageTerm) {
		String dataDivider = storageTermData.dataDivider;
		ensureCollectedDataForDataDivider(dataDivider);
		collectedDataByDataDivider.get(dataDivider).addChild(storageTerm);
	}

	private void ensureCollectedDataForDataDivider(String dataDivider) {
		if (!collectedDataByDataDivider.containsKey(dataDivider)) {
			DataGroup collectedData = DataGroup.withNameInData("collectedData");
			collectedDataByDataDivider.put(dataDivider, collectedData);
		}
	}
}