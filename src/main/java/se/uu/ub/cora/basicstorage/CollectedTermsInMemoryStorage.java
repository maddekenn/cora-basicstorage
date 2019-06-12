/*
 * Copyright 2017 Uppsala University Library
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
package se.uu.ub.cora.basicstorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import se.uu.ub.cora.data.DataGroup;

class CollectedTermsInMemoryStorage {
	private Map<String, Map<String, Map<String, List<StorageTermData>>>> terms = new HashMap<>();

	void removePreviousCollectedStorageTerms(String recordType, String recordId) {
		if (termsExistForRecordType(recordType)) {
			Map<String, Map<String, List<StorageTermData>>> termsForRecordType = terms
					.get(recordType);
			removePreviousCollectedStorageTermsForRecordType(recordId, termsForRecordType);
		}
	}

	private boolean termsExistForRecordType(String recordType) {
		return terms.containsKey(recordType);
	}

	private void removePreviousCollectedStorageTermsForRecordType(String recordId,
			Map<String, Map<String, List<StorageTermData>>> termsForRecordType) {
		for (Entry<String, Map<String, List<StorageTermData>>> keyEntry : termsForRecordType
				.entrySet()) {
			Map<String, List<StorageTermData>> termsForRecordId = keyEntry.getValue();
			removePreviousCollectedStorageTermsForRecordId(recordId, termsForRecordId);
		}
	}

	private void removePreviousCollectedStorageTermsForRecordId(String recordId,
			Map<String, List<StorageTermData>> termsForRecordId) {
		List<String> idsToRemove = new ArrayList<>();
		findIdsToRemove(recordId, termsForRecordId, idsToRemove);
		removeStorageTermsForIds(termsForRecordId, idsToRemove);
	}

	private void findIdsToRemove(String recordId,
			Map<String, List<StorageTermData>> termsForRecordId, List<String> idsToRemove) {
		for (Entry<String, List<StorageTermData>> recordIdEntry : termsForRecordId.entrySet()) {
			if (termsExistForRecordId(recordId, recordIdEntry)) {
				idsToRemove.add(recordIdEntry.getKey());
			}
		}
	}

	private void removeStorageTermsForIds(Map<String, List<StorageTermData>> termsForRecordId,
			List<String> idsToRemove) {
		for (String key : idsToRemove) {
			termsForRecordId.remove(key);
		}
	}

	private boolean termsExistForRecordId(String recordId,
			Entry<String, List<StorageTermData>> recordIdEntry) {
		return recordIdEntry.getKey().equals(recordId);
	}

	void storeCollectedTerms(String recordType, String recordId, DataGroup collectedTerms,
			String dataDivider) {
		removePreviousCollectedStorageTerms(recordType, recordId);
		if (collectedTerms.containsChildWithNameInData("storage")) {
			storeCollectedStorageTerms(recordType, recordId, collectedTerms, dataDivider);
		}
	}

	private void storeCollectedStorageTerms(String recordType, String recordId,
			DataGroup collectedTerms, String dataDivider) {
		DataGroup collectStorageTerm = collectedTerms.getFirstGroupWithNameInData("storage");
		for (DataGroup collectedDataTerm : collectStorageTerm
				.getAllGroupsWithNameInData("collectedDataTerm")) {
			storeCollectedStorageTerm(recordType, recordId, dataDivider, collectedDataTerm);
		}
	}

	private void storeCollectedStorageTerm(String recordType, String recordId, String dataDivider,
			DataGroup collectedDataTerm) {
		DataGroup extraData = collectedDataTerm.getFirstGroupWithNameInData("extraData");
		String storageKey = extraData.getFirstAtomicValueWithNameInData("storageKey");
		String termValue = collectedDataTerm.getFirstAtomicValueWithNameInData("collectTermValue");

		List<StorageTermData> listOfStorageTermData = ensureStorageListExistsForTermForTypeAndKeyAndId(
				recordType, storageKey, recordId);

		listOfStorageTermData.add(StorageTermData.withValueAndDataDivider(termValue, dataDivider));
	}

	void storeCollectedStorageTermData(String recordType, String storageKey, String recordId,
			StorageTermData storageTermData) {
		List<StorageTermData> listOfStorageTermData = ensureStorageListExistsForTermForTypeAndKeyAndId(
				recordType, storageKey, recordId);

		listOfStorageTermData.add(storageTermData);
	}

	private List<StorageTermData> ensureStorageListExistsForTermForTypeAndKeyAndId(
			String recordType, String storageKey, String recordId) {
		ensureStorageMapExistsForRecordType(recordType);
		Map<String, Map<String, List<StorageTermData>>> storageKeysForType = terms.get(recordType);
		ensureStorageListExistsForTermKey(storageKey, storageKeysForType);
		ensureStorageListExistsForId(storageKey, recordId, storageKeysForType);
		return storageKeysForType.get(storageKey).get(recordId);
	}

	private void ensureStorageMapExistsForRecordType(String recordType) {
		if (!terms.containsKey(recordType)) {
			terms.put(recordType, new HashMap<>());
		}
	}

	private void ensureStorageListExistsForTermKey(String storageKey,
			Map<String, Map<String, List<StorageTermData>>> storageKeysForType) {
		if (!storageKeysForType.containsKey(storageKey)) {
			HashMap<String, List<StorageTermData>> mapOfIds = new HashMap<>();
			storageKeysForType.put(storageKey, mapOfIds);
		}
	}

	private void ensureStorageListExistsForId(String storageKey, String recordId,
			Map<String, Map<String, List<StorageTermData>>> storageKeysForType) {
		if (!storageKeysForType.get(storageKey).containsKey(recordId)) {
			storageKeysForType.get(storageKey).put(recordId, new ArrayList<>());
		}
	}

	List<String> findRecordIdsForFilter(String type, DataGroup filter) {
		DataGroup filterPart = filter.getFirstGroupWithNameInData("part");
		if (terms.containsKey(type)) {
			return findRecordIdsMatchingFilterPart(type, filterPart);
		}
		return Collections.emptyList();
	}

	private List<String> findRecordIdsMatchingFilterPart(String type, DataGroup filterPart) {
		String key = filterPart.getFirstAtomicValueWithNameInData("key");
		String value = filterPart.getFirstAtomicValueWithNameInData("value");
		Map<String, Map<String, List<StorageTermData>>> storageTermsForRecordType = terms.get(type);
		if (storageTermsForRecordType.containsKey(key)) {

			Map<String, List<StorageTermData>> mapOfIdsAndStorageTermForTypeAndKey = storageTermsForRecordType
					.get(key);
			return findRecordIdsMatchingValueForKey(value, mapOfIdsAndStorageTermForTypeAndKey);
		}
		return Collections.emptyList();
	}

	private List<String> findRecordIdsMatchingValueForKey(String value,
			Map<String, List<StorageTermData>> mapOfIdsAndStorageTermForTypeAndKey) {
		List<String> foundRecordIdsForKey = new ArrayList<>();

		for (Entry<String, List<StorageTermData>> entry : mapOfIdsAndStorageTermForTypeAndKey
				.entrySet()) {
			findRecordIdsMatchingValueForId(value, foundRecordIdsForKey, entry);
		}

		return foundRecordIdsForKey;
	}

	private void findRecordIdsMatchingValueForId(String value, List<String> foundRecordIdsForKey,
			Entry<String, List<StorageTermData>> entry) {
		String key = entry.getKey();
		for (StorageTermData storageTermData : entry.getValue()) {
			if (storageTermData.value.equals(value)) {
				foundRecordIdsForKey.add(key);
			}
		}
	}

	Map<String, DataGroup> structureCollectedTermsForDisk() {
		return new CollectedDataOrganiser().structureCollectedDataForDisk(terms);
	}

}