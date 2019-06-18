/*
 * Copyright 2015, 2017, 2018 Uppsala University Library
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import se.uu.ub.cora.bookkeeper.metadata.MetadataTypes;
import se.uu.ub.cora.bookkeeper.storage.MetadataStorage;
import se.uu.ub.cora.data.DataElement;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.spider.data.SpiderDataGroup;
import se.uu.ub.cora.spider.record.DataGroupEnhancer;
import se.uu.ub.cora.spider.record.storage.RecordConflictException;
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.SearchStorage;
import se.uu.ub.cora.storage.SpiderReadResult;

public class RecordStorageInMemory implements RecordStorage, MetadataStorage, SearchStorage {
	private static final String RECORD_TYPE = "recordType";
	private static final String NO_RECORDS_EXISTS_MESSAGE = "No records exists with recordType: ";

	private DataGroup emptyFilter = DataGroup.withNameInData("filter");
	protected Map<String, Map<String, DividerGroup>> records = new HashMap<>();
	protected CollectedTermsInMemoryStorage collectedTermsHolder = new CollectedTermsInMemoryStorage();
	protected Map<String, Map<String, DividerGroup>> linkLists = new HashMap<>();
	protected Map<String, Map<String, Map<String, Map<String, List<DataGroup>>>>> incomingLinks = new HashMap<>();

	public RecordStorageInMemory() {
		// Make it possible to use default empty record storage
	}

	public RecordStorageInMemory(Map<String, Map<String, DividerGroup>> records) {
		throwErrorIfConstructorArgumentIsNull(records);
		this.records = records;
	}

	private void throwErrorIfConstructorArgumentIsNull(
			Map<String, Map<String, DividerGroup>> records) {
		if (null == records) {
			throw new IllegalArgumentException("Records must not be null");
		}
	}

	@Override
	public void create(String recordType, String recordId, DataGroup record,
			DataGroup collectedTerms, DataGroup linkList, String dataDivider) {
		ensureStorageExistsForRecordType(recordType);
		checkNoConflictOnRecordId(recordType, recordId);
		storeIndependentRecordByRecordTypeAndRecordId(recordType, recordId, record, dataDivider);
		collectedTermsHolder.storeCollectedTerms(recordType, recordId, collectedTerms, dataDivider);
		storeLinks(recordType, recordId, linkList, dataDivider);
	}

	protected void ensureStorageExistsForRecordType(String recordType) {
		if (holderForRecordTypeDoesNotExistInStorage(recordType)) {
			createHolderForRecordTypeInStorage(recordType);
		}
	}

	private boolean holderForRecordTypeDoesNotExistInStorage(String recordType) {
		return !records.containsKey(recordType);
	}

	private void createHolderForRecordTypeInStorage(String recordType) {
		records.put(recordType, new HashMap<String, DividerGroup>());
		linkLists.put(recordType, new HashMap<String, DividerGroup>());
	}

	private void checkNoConflictOnRecordId(String recordType, String recordId) {
		if (recordIdExistsForRecordType(recordType, recordId)) {
			throw new RecordConflictException(
					"Record with recordId: " + recordId + " already exists");
		}
	}

	private void storeIndependentRecordByRecordTypeAndRecordId(String recordType, String recordId,
			DataGroup record, String dataDivider) {
		DataGroup recordIndependentOfEnteredRecord = createIndependentCopy(record);
		storeRecordByRecordTypeAndRecordId(recordType, recordId, recordIndependentOfEnteredRecord,
				dataDivider);
	}

	private DataGroup createIndependentCopy(DataGroup record) {
		DataGroupEnhancer dataGroupEnhancer = new DataGroupEnhancer();
		dataGroupEnhancer.enhance(record);
		return SpiderDataGroup.fromDataGroup(record).toDataGroup();
	}

	protected void storeRecordByRecordTypeAndRecordId(String recordType, String recordId,
			DataGroup recordIndependentOfEnteredRecord, String dataDivider) {
		records.get(recordType).put(recordId, DividerGroup.withDataDividerAndDataGroup(dataDivider,
				recordIndependentOfEnteredRecord));
	}

	protected void storeLinks(String recordType, String recordId, DataGroup linkList,
			String dataDivider) {
		if (!linkList.getChildren().isEmpty()) {
			DataGroup linkListIndependentFromEntered = createIndependentCopy(linkList);
			storeLinkList(recordType, recordId, linkListIndependentFromEntered, dataDivider);
			storeLinksInIncomingLinks(linkListIndependentFromEntered);
		} else {
			if (!linksMissingForRecord(recordType, recordId)) {
				linkLists.get(recordType).remove(recordId);
			}
		}
	}

	private void storeLinkList(String recordType, String recordId,
			DataGroup linkListIndependentFromEntered, String dataDivider) {
		Map<String, DividerGroup> linksForRecordType = linkLists.get(recordType);
		linksForRecordType.put(recordId, DividerGroup.withDataDividerAndDataGroup(dataDivider,
				linkListIndependentFromEntered));
	}

	private void storeLinksInIncomingLinks(DataGroup incomingLinkList) {
		for (DataElement linkElement : incomingLinkList.getChildren()) {
			storeLinkInIncomingLinks((DataGroup) linkElement);
		}
	}

	private void storeLinkInIncomingLinks(DataGroup link) {
		Map<String, Map<String, List<DataGroup>>> toPartOfIncomingLinks = getIncomingLinkStorageForLink(
				link);
		storeLinkInIncomingLinks(link, toPartOfIncomingLinks);
	}

	private Map<String, Map<String, List<DataGroup>>> getIncomingLinkStorageForLink(
			DataGroup link) {
		DataGroup to = link.getFirstGroupWithNameInData("to");
		String toType = extractLinkedRecordTypeValue(to);
		String toId = extractLinkedRecordIdValue(to);

		ensureInIncomingLinksHolderForRecordTypeAndRecordId(toType, toId);

		return incomingLinks.get(toType).get(toId);
	}

	private String extractLinkedRecordIdValue(DataGroup to) {
		return to.getFirstAtomicValueWithNameInData("linkedRecordId");
	}

	private String extractLinkedRecordTypeValue(DataGroup dataGroup) {
		return dataGroup.getFirstAtomicValueWithNameInData("linkedRecordType");
	}

	private void ensureInIncomingLinksHolderForRecordTypeAndRecordId(String toType, String toId) {
		if (isIncomingLinksHolderForRecordTypeMissing(toType)) {
			incomingLinks.put(toType, new HashMap<>());
		}
		if (isIncomingLinksHolderForRecordIdMissing(toType, toId)) {
			incomingLinks.get(toType).put(toId, new HashMap<>());
		}
	}

	private boolean isIncomingLinksHolderForRecordTypeMissing(String toType) {
		return !incomingLinkStorageForRecordTypeExists(toType);
	}

	private boolean isIncomingLinksHolderForRecordIdMissing(String toType, String toId) {
		return !incomingLinksHolderForRecordIdExists(toType, toId);
	}

	private void storeLinkInIncomingLinks(DataGroup link,
			Map<String, Map<String, List<DataGroup>>> toPartOfIncomingLinks) {
		DataGroup from = link.getFirstGroupWithNameInData("from");
		String fromType = extractLinkedRecordTypeValue(from);
		String fromId = extractLinkedRecordIdValue(from);

		ensureIncomingLinksHolderExistsForFromRecordType(toPartOfIncomingLinks, fromType);

		ensureIncomingLinksHolderExistsForFromRecordId(toPartOfIncomingLinks.get(fromType), fromId);
		toPartOfIncomingLinks.get(fromType).get(fromId).add(link);
	}

	private void ensureIncomingLinksHolderExistsForFromRecordType(
			Map<String, Map<String, List<DataGroup>>> toPartOfIncomingLinks, String fromType) {
		if (!toPartOfIncomingLinks.containsKey(fromType)) {
			toPartOfIncomingLinks.put(fromType, new HashMap<>());
		}
	}

	private void ensureIncomingLinksHolderExistsForFromRecordId(
			Map<String, List<DataGroup>> fromPartOfIncomingLinks, String fromId) {
		if (!fromPartOfIncomingLinks.containsKey(fromId)) {
			fromPartOfIncomingLinks.put(fromId, new ArrayList<>());
		}
	}

	@Override
	public SpiderReadResult readList(String type, DataGroup filter) {
		Map<String, DividerGroup> typeDividerRecords = records.get(type);
		throwErrorIfNoRecordOfType(type, typeDividerRecords);

		return getSpiderReadResult(type, filter, typeDividerRecords);
	}

	private SpiderReadResult getSpiderReadResult(String type, DataGroup filter,
			Map<String, DividerGroup> typeDividerRecords) {
		SpiderReadResult spiderReadResult = new SpiderReadResult();
		Collection<DataGroup> readFromList = readFromList(type, filter, typeDividerRecords);
		spiderReadResult.listOfDataGroups = new ArrayList<>(readFromList);
		spiderReadResult.totalNumberOfMatches = readFromList.size();
		return spiderReadResult;
	}

	private Collection<DataGroup> readFromList(String type, DataGroup filter,
			Map<String, DividerGroup> typeDividerRecords) {
		if (filterIsEmpty(filter)) {
			return readListWithoutFilter(typeDividerRecords);
		}
		return readListWithFilter(type, filter);
	}

	private Collection<DataGroup> readListWithoutFilter(
			Map<String, DividerGroup> typeDividerRecords) {
		Map<String, DataGroup> typeRecords = addDataGroupToRecordTypeList(typeDividerRecords);
		return typeRecords.values();
	}

	private Collection<DataGroup> readListWithFilter(String type, DataGroup filter) {
		List<String> foundRecordIdsForFilter = collectedTermsHolder.findRecordIdsForFilter(type,
				filter);
		return readRecordsForTypeAndListOfIds(type, foundRecordIdsForFilter);
	}

	private Collection<DataGroup> readRecordsForTypeAndListOfIds(String type,
			List<String> foundRecordIdsForFilter) {
		List<DataGroup> foundRecords = new ArrayList<>();
		for (String foundRecordId : foundRecordIdsForFilter) {
			foundRecords.add(read(type, foundRecordId));
		}
		return foundRecords;
	}

	private void throwErrorIfNoRecordOfType(String type,
			Map<String, DividerGroup> typeDividerRecords) {
		if (null == typeDividerRecords) {
			throw new RecordNotFoundException(NO_RECORDS_EXISTS_MESSAGE + type);
		}
	}

	private boolean filterIsEmpty(DataGroup filter) {
		return !filter.containsChildWithNameInData("part");
	}

	private Map<String, DataGroup> addDataGroupToRecordTypeList(
			Map<String, DividerGroup> typeDividerRecords) {
		Map<String, DataGroup> typeRecords = new HashMap<>();
		for (Entry<String, DividerGroup> entry : typeDividerRecords.entrySet()) {
			typeRecords.put(entry.getKey(), entry.getValue().dataGroup);
		}
		return typeRecords;
	}

	@Override
	public SpiderReadResult readAbstractList(String type, DataGroup filter) {
		List<DataGroup> aggregatedRecordList = new ArrayList<>();
		List<String> implementingChildRecordTypes = findImplementingChildRecordTypes(type);

		addRecordsToAggregatedRecordList(aggregatedRecordList, implementingChildRecordTypes,
				filter);
		addRecordsForParentIfParentIsNotAbstract(type, filter, aggregatedRecordList);
		throwErrorIfEmptyAggregatedList(type, aggregatedRecordList);
		SpiderReadResult spiderReadResult = new SpiderReadResult();
		spiderReadResult.listOfDataGroups = aggregatedRecordList;
		spiderReadResult.totalNumberOfMatches = aggregatedRecordList.size();
		return spiderReadResult;
	}

	private List<String> findImplementingChildRecordTypes(String type) {
		Map<String, DividerGroup> allRecordTypes = records.get(RECORD_TYPE);
		List<String> implementingRecordTypes = new ArrayList<>();
		return findImplementingChildRecordTypesUsingTypeAndRecordTypeList(type, allRecordTypes,
				implementingRecordTypes);
	}

	private List<String> findImplementingChildRecordTypesUsingTypeAndRecordTypeList(String type,
			Map<String, DividerGroup> allRecordTypes, List<String> implementingRecordTypes) {
		for (Entry<String, DividerGroup> entry : allRecordTypes.entrySet()) {
			checkIfChildAndAddToList(type, implementingRecordTypes, entry);
		}
		return implementingRecordTypes;
	}

	private void checkIfChildAndAddToList(String type, List<String> implementingRecordTypes,
			Entry<String, DividerGroup> entry) {
		DataGroup dataGroup = extractDataGroupFromDataDividerGroup(entry);
		String recordTypeId = entry.getKey();

		if (isImplementingChild(type, dataGroup)) {
			implementingRecordTypes.add(recordTypeId);
			findImplementingChildRecordTypesUsingTypeAndRecordTypeList(entry.getKey(),
					records.get(RECORD_TYPE), implementingRecordTypes);
		}
	}

	private DataGroup extractDataGroupFromDataDividerGroup(Entry<String, DividerGroup> entry) {
		DividerGroup dividerGroup = entry.getValue();
		return dividerGroup.dataGroup;
	}

	private boolean isImplementingChild(String type, DataGroup dataGroup) {
		if (dataGroup.containsChildWithNameInData("parentId")) {
			String parentId = extractParentId(dataGroup);
			if (parentId.equals(type)) {
				return true;
			}
		}
		return false;
	}

	private String extractParentId(DataGroup dataGroup) {
		DataGroup parent = dataGroup.getFirstGroupWithNameInData("parentId");
		return parent.getFirstAtomicValueWithNameInData("linkedRecordId");
	}

	private void addRecordsToAggregatedRecordList(List<DataGroup> aggregatedRecordList,
			List<String> implementingChildRecordTypes, DataGroup filter) {
		for (String implementingRecordType : implementingChildRecordTypes) {
			try {
				readRecordsForTypeAndFilterAndAddToList(implementingRecordType, filter,
						aggregatedRecordList);
			} catch (RecordNotFoundException e) {
				// Do nothing, another implementing child might have records
			}
		}
	}

	private void readRecordsForTypeAndFilterAndAddToList(String implementingRecordType,
			DataGroup filter, List<DataGroup> aggregatedRecordList) {
		Collection<DataGroup> readList = readList(implementingRecordType, filter).listOfDataGroups;
		aggregatedRecordList.addAll(readList);
	}

	private boolean parentRecordTypeIsNotAbstract(DataGroup recordTypeDataGroup) {
		return !recordTypeIsAbstract(recordTypeDataGroup);
	}

	private void addRecordsForParentIfParentIsNotAbstract(String type, DataGroup filter,
			List<DataGroup> aggregatedRecordList) {
		DataGroup recordTypeDataGroup = read(RECORD_TYPE, type);
		if (parentRecordTypeIsNotAbstract(recordTypeDataGroup)) {
			readRecordsForTypeAndFilterAndAddToList(type, filter, aggregatedRecordList);
		}
	}

	private void throwErrorIfEmptyAggregatedList(String type,
			List<DataGroup> aggregatedRecordList) {
		if (aggregatedRecordList.isEmpty()) {
			throw new RecordNotFoundException(NO_RECORDS_EXISTS_MESSAGE + type);
		}
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String recordType,
			String recordId) {
		return recordExistsForRecordTypeAndRecordId(recordType, recordId)
				|| recordExistsForAbstractRecordTypeAndRecordId(recordType, recordId);
	}

	@Override
	public boolean recordsExistForRecordType(String type) {
		return records.get(type) != null;
	}

	private boolean recordExistsForRecordTypeAndRecordId(String recordType, String recordId) {
		return recordsExistForRecordType(recordType)
				&& recordIdExistsForRecordType(recordType, recordId);
	}

	private boolean recordExistsForAbstractRecordTypeAndRecordId(String recordType,
			String recordId) {
		return recordsExistForRecordType(RECORD_TYPE)
				&& recordTypeExistsAndIsAbstractAndRecordIdExistInImplementingChild(recordType,
						recordId);
	}

	private boolean recordIdExistsForRecordType(String recordType, String recordId) {
		return records.get(recordType).containsKey(recordId);
	}

	private boolean recordTypeExistsAndIsAbstractAndRecordIdExistInImplementingChild(
			String recordType, String recordId) {
		if (recordTypeDoesNotExist(recordType)) {
			return false;
		}
		return recordTypeIsAbstractAndRecordIdExistInImplementingChild(recordType, recordId);
	}

	private boolean recordTypeDoesNotExist(String recordType) {
		return !recordExistsForRecordTypeAndRecordId(RECORD_TYPE, recordType);
	}

	private boolean recordTypeIsAbstractAndRecordIdExistInImplementingChild(String recordType,
			String recordId) {
		DataGroup recordTypeDataGroup = read(RECORD_TYPE, recordType);
		if (recordTypeIsAbstract(recordTypeDataGroup)) {
			return checkIfRecordIdExistsInChildren(recordType, recordId);
		}
		return false;
	}

	private boolean recordTypeIsAbstract(DataGroup recordTypeDataGroup) {
		String abstractValue = recordTypeDataGroup.getFirstAtomicValueWithNameInData("abstract");
		return valueIsAbstract(abstractValue);
	}

	private boolean valueIsAbstract(String typeIsAbstract) {
		return "true".equals(typeIsAbstract);
	}

	private boolean checkIfRecordIdExistsInChildren(String recordType, String recordId) {
		List<String> implementingChildRecordTypes = findImplementingChildRecordTypes(recordType);
		for (String childType : implementingChildRecordTypes) {
			if (recordsExistForRecordType(childType)
					&& recordIdExistsForRecordType(childType, recordId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public DataGroup read(String recordType, String recordId) {
		DataGroup recordTypeDataGroup = returnRecordIfExisting(RECORD_TYPE, recordType);
		if (recordTypeIsAbstract(recordTypeDataGroup)) {
			return readRecordFromImplementingRecordTypes(recordType, recordId);
		}
		return returnRecordIfExisting(recordType, recordId);
	}

	private DataGroup readRecordFromImplementingRecordTypes(String recordType, String recordId) {
		DataGroup readRecord = tryToReadRecordFromImplementingRecordTypes(recordType, recordId);
		if (readRecord == null) {
			throw new RecordNotFoundException("No record exists with recordId: " + recordId);
		}
		return readRecord;
	}

	private DataGroup tryToReadRecordFromImplementingRecordTypes(String recordType,
			String recordId) {
		DataGroup readRecord = null;
		List<String> implementingChildRecordTypes = findImplementingChildRecordTypes(recordType);
		for (String implementingType : implementingChildRecordTypes) {
			try {
				readRecord = returnRecordIfExisting(implementingType, recordId);
			} catch (RecordNotFoundException e) {
				// Do nothing, another implementing child might have records
			}
		}
		return readRecord;
	}

	private DataGroup returnRecordIfExisting(String recordType, String recordId) {
		checkRecordExists(recordType, recordId);
		return records.get(recordType).get(recordId).dataGroup;
	}

	private void checkRecordExists(String recordType, String recordId) {
		if (holderForRecordTypeDoesNotExistInStorage(recordType)) {
			throw new RecordNotFoundException(NO_RECORDS_EXISTS_MESSAGE + recordType);
		}
		if (null == records.get(recordType).get(recordId)) {
			throw new RecordNotFoundException("No record exists with recordId: " + recordId);
		}
	}

	@Override
	public DataGroup readLinkList(String recordType, String recordId) {
		checkRecordExists(recordType, recordId);
		if (linksMissingForRecord(recordType, recordId)) {
			return DataGroup.withNameInData("collectedDataLinks");
		}
		return linkLists.get(recordType).get(recordId).dataGroup;
	}

	private boolean linksMissingForRecord(String recordType, String recordId) {
		return !linkLists.get(recordType).containsKey(recordId);
	}

	@Override
	public void deleteByTypeAndId(String recordType, String recordId) {
		checkRecordExists(recordType, recordId);
		removeOldLinksStoredAsIncomingLinks(recordType, recordId);
		removeFromLinkList(recordType, recordId);
		collectedTermsHolder.removePreviousCollectedStorageTerms(recordType, recordId);
		records.get(recordType).remove(recordId);
		if (records.get(recordType).isEmpty()) {
			records.remove(recordType);
		}

	}

	private void removeFromLinkList(String recordType, String recordId) {
		if (!linksMissingForRecord(recordType, recordId)) {
			linkLists.get(recordType).remove(recordId);
			if (linkLists.get(recordType).isEmpty()) {
				linkLists.remove(recordType);
			}
		}
	}

	@Override
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		if (linksExistForRecord(type, id)) {
			return generateLinkCollectionFromStoredLinks(type, id);
		}
		return Collections.emptyList();
	}

	private Collection<DataGroup> generateLinkCollectionFromStoredLinks(String type, String id) {
		List<DataGroup> generatedLinkList = new ArrayList<>();
		Map<String, Map<String, List<DataGroup>>> linkStorageForRecord = incomingLinks.get(type)
				.get(id);
		addLinksForRecordFromAllRecordTypes(generatedLinkList, linkStorageForRecord);
		return generatedLinkList;
	}

	private void addLinksForRecordFromAllRecordTypes(List<DataGroup> generatedLinkList,
			Map<String, Map<String, List<DataGroup>>> linkStorageForRecord) {
		for (Map<String, List<DataGroup>> mapOfId : linkStorageForRecord.values()) {
			addLinksForRecordForThisRecordType(generatedLinkList, mapOfId);
		}
	}

	private void addLinksForRecordForThisRecordType(List<DataGroup> generatedLinkList,
			Map<String, List<DataGroup>> mapOfId) {
		for (List<DataGroup> recordToRecordLinkList : mapOfId.values()) {
			generatedLinkList.addAll(recordToRecordLinkList);
		}
	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		return incomingLinkStorageForRecordTypeExists(type)
				&& incomingLinksHolderForRecordIdExists(type, id);
	}

	private boolean incomingLinksHolderForRecordIdExists(String type, String id) {
		return incomingLinks.get(type).containsKey(id);
	}

	private boolean incomingLinkStorageForRecordTypeExists(String type) {
		return incomingLinks.containsKey(type);
	}

	@Override
	public void update(String recordType, String recordId, DataGroup record,
			DataGroup collectedTerms, DataGroup linkList, String dataDivider) {
		checkRecordExists(recordType, recordId);
		removeOldLinksStoredAsIncomingLinks(recordType, recordId);
		storeIndependentRecordByRecordTypeAndRecordId(recordType, recordId, record, dataDivider);
		ensureStorageExistsForRecordType(recordType);
		collectedTermsHolder.storeCollectedTerms(recordType, recordId, collectedTerms, dataDivider);
		storeLinks(recordType, recordId, linkList, dataDivider);
	}

	private void removeOldLinksStoredAsIncomingLinks(String recordType, String recordId) {
		DataGroup oldLinkList = readLinkList(recordType, recordId);
		for (DataElement linkElement : oldLinkList.getChildren()) {
			removeOldLinkStoredAsIncomingLink(linkElement);
		}
	}

	private void removeOldLinkStoredAsIncomingLink(DataElement linkElement) {
		DataGroup link = (DataGroup) linkElement;
		DataGroup toPartOfLink = link.getFirstGroupWithNameInData("to");
		String toType = extractLinkedRecordTypeValue(toPartOfLink);
		String toId = extractLinkedRecordIdValue(toPartOfLink);

		if (incomingLinksContainsToTypeAndToId(toType, toId)) {
			Map<String, Map<String, List<DataGroup>>> toPartOfIncomingLinks = getFromPartOfIncomingLinksForToTypeAndToId(
					toType, toId);

			removeLinkAndFromHolderFromIncomingLinks(link, toPartOfIncomingLinks);

			removeToHolderFromIncomingLinks(toType, toId, toPartOfIncomingLinks);
		}
	}

	private boolean incomingLinksContainsToTypeAndToId(String toType, String toId) {
		if (!incomingLinks.containsKey(toType)) {
			return false;
		}

		return incomingLinks.get(toType).containsKey(toId);
	}

	private Map<String, Map<String, List<DataGroup>>> getFromPartOfIncomingLinksForToTypeAndToId(
			String toType, String toId) {
		return incomingLinks.get(toType).get(toId);
	}

	private void removeLinkAndFromHolderFromIncomingLinks(DataGroup link,
			Map<String, Map<String, List<DataGroup>>> linksForToPart) {
		DataGroup from = link.getFirstGroupWithNameInData("from");
		String fromType = extractLinkedRecordTypeValue(from);
		String fromId = extractLinkedRecordIdValue(from);

		if (linksForToPart.containsKey(fromType)) {
			linksForToPart.get(fromType).remove(fromId);

			if (linksForToPart.get(fromType).isEmpty()) {
				linksForToPart.remove(fromType);
			}
		}
	}

	private void removeToHolderFromIncomingLinks(String toType, String toId,
			Map<String, Map<String, List<DataGroup>>> toPartOfIncomingLinks) {
		if (toPartOfIncomingLinks.isEmpty()) {
			incomingLinks.get(toType).remove(toId);
		}
		if (incomingLinks.get(toType).isEmpty()) {
			incomingLinks.remove(toType);
		}
	}

	@Override
	public Collection<DataGroup> getMetadataElements() {
		Collection<DataGroup> readDataGroups = new ArrayList<>();
		for (MetadataTypes metadataType : MetadataTypes.values()) {
			readListForMetadataType(readDataGroups, metadataType);
		}
		return readDataGroups;
	}

	private void readListForMetadataType(Collection<DataGroup> readDataGroups,
			MetadataTypes metadataType) {
		DataGroup recordTypeDataGroup = read(RECORD_TYPE, metadataType.type);
		if (recordTypeIsAbstract(recordTypeDataGroup)) {
			readDataGroups
					.addAll(readAbstractList(metadataType.type, emptyFilter).listOfDataGroups);
		} else {
			readDataGroups.addAll(readList(metadataType.type, emptyFilter).listOfDataGroups);
		}
	}

	@Override
	public Collection<DataGroup> getPresentationElements() {
		return readList("presentation", emptyFilter).listOfDataGroups;
	}

	@Override
	public Collection<DataGroup> getTexts() {
		return readList("text", emptyFilter).listOfDataGroups;
	}

	@Override
	public Collection<DataGroup> getRecordTypes() {
		return readList(RECORD_TYPE, emptyFilter).listOfDataGroups;
	}

	@Override
	public Collection<DataGroup> getCollectTerms() {
		return readAbstractList("collectTerm", emptyFilter).listOfDataGroups;
	}

	@Override
	public DataGroup getSearchTerm(String searchTermId) {
		return read("searchTerm", searchTermId);
	}

	@Override
	public DataGroup getCollectIndexTerm(String collectIndexTermId) {
		return read("collectIndexTerm", collectIndexTermId);
	}

}
