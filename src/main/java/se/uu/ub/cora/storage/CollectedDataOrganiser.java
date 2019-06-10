/*
 * Copyright 2017, 2019 Uppsala University Library
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import se.uu.ub.cora.data.DataAtomic;
import se.uu.ub.cora.data.DataGroup;

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

	private void loopKeysAndCreateStorageTerms(
			Map<String, Map<String, List<StorageTermData>>> map) {
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

	private void loopStorageTermDataAndCreateStorageTerms(
			Entry<String, List<StorageTermData>> idEntry) {
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
		storageTerm.addChild(
				DataAtomic.withNameInDataAndValue("dataDivider", storageTermData.dataDivider));
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