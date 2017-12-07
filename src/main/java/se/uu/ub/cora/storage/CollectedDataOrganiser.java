package se.uu.ub.cora.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;

class CollectedDataOrganiser {
	private Map<String, DataGroup> collectedDataByDataDivider;

	protected Map<String, DataGroup> structureCollectedDataForDisk(
			Map<String, Map<String, List<StorageTermData>>> collectedDataTerms) {
		collectedDataByDataDivider = new HashMap<>();
		for (Entry<String, Map<String, List<StorageTermData>>> entryRecordType : collectedDataTerms
				.entrySet()) {
			loopRecordTypesAndCreateStorageTerms(entryRecordType);
		}
		return collectedDataByDataDivider;
	}

	private void loopRecordTypesAndCreateStorageTerms(
			Entry<String, Map<String, List<StorageTermData>>> entryRecordType) {
		String recordType = entryRecordType.getKey();
		Map<String, List<StorageTermData>> mapForRecordType = entryRecordType.getValue();
		for (Entry<String, List<StorageTermData>> mapForEntryKey : mapForRecordType.entrySet()) {
			loopKeysAndCreateStorageTerm(recordType, mapForEntryKey);
		}
	}

	private void loopKeysAndCreateStorageTerm(String recordType,
			Entry<String, List<StorageTermData>> mapForEntryKey) {
		String key = mapForEntryKey.getKey();

		int repeatId2 = 0;
		for (StorageTermData storageTermData : mapForEntryKey.getValue()) {
			DataGroup storageTerm = createStorageTerm(recordType, key, repeatId2, storageTermData);

			String dataDivider = storageTermData.dataDivider;

			ensureCollectedDataForDataDivider(dataDivider);
			collectedDataByDataDivider.get(dataDivider).addChild(storageTerm);
			repeatId2++;
		}
	}

	private DataGroup createStorageTerm(String recordType, String key, int repeatId2,
			StorageTermData storageTermData) {
		DataGroup storageTerm = DataGroup.withNameInData("storageTerm");
		storageTerm.setRepeatId(String.valueOf(repeatId2));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("type", recordType));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("key", key));

		storageTerm.addChild(DataAtomic.withNameInDataAndValue("value", storageTermData.value));
		storageTerm.addChild(DataAtomic.withNameInDataAndValue("id", storageTermData.id));
		storageTerm
				.addChild(DataAtomic.withNameInDataAndValue("dataDivider", storageTermData.dataDivider));
		return storageTerm;
	}

	private void ensureCollectedDataForDataDivider(String dataDivider) {
		if (!collectedDataByDataDivider.containsKey(dataDivider)) {
			DataGroup collectedData = DataGroup.withNameInData("collectedData");
			collectedDataByDataDivider.put(dataDivider, collectedData);
		}
	}
}