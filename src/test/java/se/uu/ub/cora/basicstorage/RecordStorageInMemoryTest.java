/*
 * Copyright 2015, 2017 Uppsala University Library
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.basicstorage.testdata.TestDataRecordInMemoryStorage;
import se.uu.ub.cora.data.DataAtomic;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordConflictException;
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;

public class RecordStorageInMemoryTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String FROM_RECORD_ID = "fromRecordId";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private RecordStorage recordStorage;
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	private DataGroup emptyCollectedData = DataCreator.createEmptyCollectedData();
	DataGroup emptyFilter = DataGroup.withNameInData("filter");
	private String dataDivider = "cora";

	@BeforeMethod
	public void beforeMethod() {
		recordStorage = new RecordStorageInMemory();
		DataGroup typeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("type", "true", "false");
		recordStorage.create("recordType", "type", typeRecordType, emptyLinkList, emptyLinkList,
				"cora");
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, emptyLinkList,
				emptyLinkList, "cora");

	}

	@Test
	public void testInitWithData() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<>();
		records.put("place", new HashMap<String, DividerGroup>());

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		records.get("place").put("place:0001",
				DividerGroup.withDataDividerAndDataGroup(dataDivider, dataGroup));

		RecordStorageInMemory recordsInMemoryWithData = new RecordStorageInMemory(records);
		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordsInMemoryWithData.create("recordType", "place", placeRecordType, emptyLinkList,
				emptyLinkList, "cora");
		assertEquals(recordsInMemoryWithData.read("place", "place:0001"), dataGroup,
				"dataGroup should be the one added on startup");

	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"nameInData", "place", "place:0001");
	}

	@Test
	public void testCreateAndReadLinkList() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);

		DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

		assertEquals(readLinkList.getChildren().size(), 2);
	}

	@Test
	public void testGenerateTwoLinksPointingToRecordFromDifferentRecords() {
		createTwoLinksPointingToSameRecordFromDifferentRecords();

		Collection<DataGroup> generatedLinksPointingToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
				generatedLinksPointingToRecord);
	}

	private void createTwoLinksPointingToSameRecordFromDifferentRecords() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);

		DataGroup linkList2 = createLinkListWithTwoLinks("fromRecordId2");
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, emptyLinkList, linkList2,
				dataDivider);
	}

	private void assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
			Collection<DataGroup> generatedLinksPointToRecord) {
		assertEquals(generatedLinksPointToRecord.size(), 2);

		Iterator<DataGroup> generatedLinks = generatedLinksPointToRecord.iterator();
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, "fromRecordId2",
				TO_RECORD_TYPE, TO_RECORD_ID);
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);

		assertNoGeneratedLinksForRecordTypeAndRecordId(TO_RECORD_TYPE, "NOT_toRecordId");
		assertNoGeneratedLinksForRecordTypeAndRecordId("NOT_toRecordType", TO_RECORD_ID);
	}

	@Test
	public void testGenerateTwoLinksPointingToSameRecordFromSameRecord() {
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		assertCorrectTwoLinksPointingToSameRecordFromSameRecord(generatedLinksPointToRecord);
	}

	private void createTwoLinksPointingToSameRecordFromSameRecord() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinksToSameRecord(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);
	}

	private DataGroup createLinkListWithTwoLinksToSameRecord(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));
		return linkList;
	}

	private void assertCorrectTwoLinksPointingToSameRecordFromSameRecord(
			Collection<DataGroup> generatedLinksPointToRecord) {
		Iterator<DataGroup> generatedLinks = generatedLinksPointToRecord.iterator();
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);
	}

	private void assertRecordLinkIsCorrect(DataGroup recordToRecordLink, String fromRecordType,
			String fromRecordId, String toRecordType, String toRecordId) {
		assertEquals(recordToRecordLink.getNameInData(), "recordToRecordLink");

		DataGroup fromOut = recordToRecordLink.getFirstGroupWithNameInData("from");

		assertEquals(fromOut.getFirstAtomicValueWithNameInData("linkedRecordType"), fromRecordType);
		assertEquals(fromOut.getFirstAtomicValueWithNameInData("linkedRecordId"), fromRecordId);

		DataGroup toOut = recordToRecordLink.getFirstGroupWithNameInData("to");
		assertEquals(toOut.getFirstAtomicValueWithNameInData("linkedRecordType"), toRecordType);
		assertEquals(toOut.getFirstAtomicValueWithNameInData("linkedRecordId"), toRecordId);
	}

	private void assertNoGeneratedLinksForRecordTypeAndRecordId(String toRecordType,
			String toRecordId) {
		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(toRecordType, toRecordId);
		assertEquals(generatedLinksPointToRecord.size(), 0);
	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	@Test
	public void testCreateWithoutLinkAndCollectedData() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyCollectedData,
				emptyLinkList, dataDivider);

		DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
		assertEquals(readLinkList.getChildren().size(), 0);
	}

	@Test
	public void testCreateAndDeleteTwoWithoutLink() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList,
				emptyLinkList, dataDivider);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID + "2", dataGroup, emptyLinkList,
				emptyLinkList, dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID + "2");
	}

	@Test
	public void testDeletedDataGroupsIdCanBeUsedToStoreAnotherDataGroup() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"recordType", "recordId");

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList,
				emptyLinkList, dataDivider);
		DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
		assertEquals(readLinkList.getChildren().size(), 0);
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData2",
						"recordType", "recordId");
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup2, emptyLinkList,
				emptyLinkList, dataDivider);
		DataGroup readLinkList2 = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
		assertEquals(readLinkList2.getChildren().size(), 0);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testInitWithemptyLinkList() {
		new RecordStorageInMemory(null);
	}

	private void createImageRecords() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0001");
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("image", "image:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0002");
		dataGroup2.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("image", "image:0002", dataGroup2, emptyLinkList, emptyLinkList,
				dataDivider);
	}

	private void createGenericBinaryRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"genericBinary", "genericBinary:0001");
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("genericBinary", "genericBinary:0001", dataGroup, emptyLinkList,
				emptyLinkList, dataDivider);
	}

	@Test
	public void testReadRecordOfAbstractType() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		DataGroup image = recordStorage.read("binary", "image:0001");
		assertNotNull(image);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadMissingRecordType() {
		recordStorage.read("", "");
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadMissingRecordId() {
		RecordStorageInMemory recordsInMemoryWithTestData = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		recordsInMemoryWithTestData.read("place", "");
	}

	@Test
	public void testCreateRead() {

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());
	}

	@Test
	public void testCreateTworecordsRead() {

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		recordStorage.create("type", "place:0002", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		DataGroup dataGroupOut2 = recordStorage.read("type", "place:0002");
		assertEquals(dataGroupOut2.getNameInData(), dataGroup.getNameInData());
	}

	@Test
	public void testCreateDataInStorageShouldBeIndependent() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		dataGroup.getChildren().clear();

		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		DataAtomic child = (DataAtomic) dataGroupOut.getChildren().get(1);

		assertEquals(child.getValue(), "childValue");
	}

	@Test(expectedExceptions = RecordConflictException.class)
	public void testCreateConflict() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("type", "place1", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		recordStorage.create("type", "place1", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
	}

	@Test
	public void testDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		recordStorage.deleteByTypeAndId("type", "place:0001");

		boolean recordFound = true;
		try {
			recordStorage.read("type", "place:0001");
			recordFound = true;

		} catch (RecordNotFoundException e) {
			recordFound = false;
		}
		assertFalse(recordFound);
	}

	@Test
	public void testGenerateTwoLinksPointingToSameRecordFromSameRecordAndThenDeletingFromRecord() {
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	@Test
	public void testGenerateMoreLinksAndThenDeletingFromRecord() {
		createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId();
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	private void createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE,
				"fromOtherRecordId", TO_RECORD_TYPE, "toOtherRecordId"));
		recordStorage.create(FROM_RECORD_TYPE, "fromOtherRecordId", dataGroup, emptyLinkList,
				linkList, dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testLinkListIsRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testLinkListIsRemovedOnDeleteRecordTypeStillExistsInLinkListStorage() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList,
				createLinkListWithLinksForTestingRemoveOfLinks(), dataDivider);
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, emptyLinkList, linkList,
				dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

	}

	private DataGroup createLinkListWithLinksForTestingRemoveOfLinks() {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID));
		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, "fromRecordId2",
				TO_RECORD_TYPE, TO_RECORD_ID));
		return linkList;
	}

	@Test
	public void testGenerateLinksPointToRecordAreRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// delete
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 0);

		assertFalse(recordStorage.linksExistForRecord(TO_RECORD_TYPE, TO_RECORD_ID));
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testDeleteNotFound() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		recordStorage.deleteByTypeAndId("type", "place:0001_NOT_FOUND");
	}

	@Test
	public void testUpdate() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		DataAtomic child = (DataAtomic) dataGroupOut.getChildren().get(1);

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		dataGroup2.addChild(DataAtomic.withNameInDataAndValue("childId2", "childValue2"));
		recordStorage.update("type", "place:0001", dataGroup2, emptyCollectedData, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut2 = recordStorage.read("type", "place:0001");
		DataAtomic child2 = (DataAtomic) dataGroupOut2.getChildren().get(1);

		assertEquals(child.getValue(), "childValue");
		assertEquals(child2.getValue(), "childValue2");
	}

	@Test
	public void testUpdateWithoutLink() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();

		recordStorage.update("place", "place:0001", dataGroup2, emptyCollectedData, emptyLinkList,
				dataDivider);

		DataGroup readLinkList = recordStorage.readLinkList("place", "place:0001");
		assertEquals(readLinkList.getChildren().size(), 0);
	}

	@Test
	public void testUpdateAndReadLinkList() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);

		DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

		assertEquals(readLinkList.getChildren().size(), 2);

		// update
		DataGroup linkListOne = createLinkListWithOneLink(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyCollectedData,
				linkListOne, dataDivider);

		DataGroup readLinkListUpdated = recordStorage.readLinkList(FROM_RECORD_TYPE,
				FROM_RECORD_ID);

		assertEquals(readLinkListUpdated.getChildren().size(), 1);
	}

	private DataGroup createLinkListWithOneLink(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		return linkList;
	}

	@Test
	public void testUpdateGenerateLinksPointToRecordAreRemovedAndAdded() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// update
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyCollectedData,
				emptyLinkList, dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 0);

		// update
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyCollectedData,
				linkList, dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
	}

	@Test
	public void testLinksFromSameRecordToSameRecordThanRemovingOne() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup linkList = createLinkListWithThreeLinksTwoOfThemFromSameRecord(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyLinkList, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 3);
		// update
		linkList = createLinkListWithTwoLinksFromDifferentRecords(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, emptyCollectedData,
				linkList, dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
	}

	private DataGroup createLinkListWithThreeLinksTwoOfThemFromSameRecord(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));
		linkList.addChild(DataCreator.createRecordToRecordLink("someOtherRecordType", fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		return linkList;
	}

	private DataGroup createLinkListWithTwoLinksFromDifferentRecords(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	private void assertNoOfLinksPointingToRecord(String toRecordType, String toRecordId,
			int expectedNoOfLinksPointingToRecord) {
		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.generateLinkCollectionPointingToRecord(toRecordType, toRecordId);
		assertEquals(generatedLinksPointToRecord.size(), expectedNoOfLinksPointingToRecord);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testUpdateNotFoundType() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.update("type", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testUpdateNotFoundId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		recordStorage.update("type", "place:0002", dataGroup, emptyCollectedData, emptyLinkList,
				dataDivider);
	}

	@Test
	public void testUpdateDataInStorageShouldBeIndependent() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);
		recordStorage.update("type", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				dataDivider);

		dataGroup.getChildren().clear();

		DataGroup dataGroupOut = recordStorage.read("type", "place:0001");
		DataAtomic child = (DataAtomic) dataGroupOut.getChildren().get(1);

		assertEquals(child.getValue(), "childValue");
	}

	@Test
	public void testRecordExistForRecordTypeAndRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		assertTrue(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId("type",
				"place:0001"));
	}

	@Test
	public void testRecordNOTExistForRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("place", "place:0004", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"place", "NOTplace:0001"));
	}

	@Test
	public void testRecordNOTExistForRecordTypeAndRecordIdMissingRecordType() {
		recordStorage = new RecordStorageInMemory();
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, emptyLinkList,
				emptyLinkList, "cora");

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"NOTtype", "place:0002"));
	}

	@Test
	public void testRecordExistForAbstractRecordTypeAndRecordId() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, emptyLinkList,
				emptyLinkList, dataDivider);

		DataGroup implementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("implementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "implementingRecordType", implementingRecordType,
				emptyLinkList, emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, emptyLinkList, emptyLinkList, dataDivider);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("implementingRecordType", "someType:0001", dataGroup, emptyLinkList,
				emptyLinkList, dataDivider);

		assertTrue(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"abstractRecordType", "someType:0001"));
	}

	@Test
	public void testRecordExistForAbstractRecordTypeAndRecordIdNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, emptyLinkList,
				emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, emptyLinkList, emptyLinkList, dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"abstractRecordType", "someType:0001"));
	}

	@Test
	public void testRecordExistForNotAbstractNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("notAbstractRecordType", "true",
						"false");
		recordStorage.create("recordType", "notAbstractRecordType", abstractRecordType,
				emptyLinkList, emptyLinkList, dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"notAbstractRecordType", "someType:0001"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"binary", "NOTimage:0004"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdRecordIdNoRecordTypeExist() {
		recordStorage = new RecordStorageInMemory();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, emptyLinkList, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
				"binary", "NOTimage:0004"));
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadWhenRecordExistForAbstractRecordTypeAndRecordIdNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, emptyLinkList,
				emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, emptyLinkList, emptyLinkList, dataDivider);

		recordStorage.read("abstractRecordType", "someType:0001");
	}
}
