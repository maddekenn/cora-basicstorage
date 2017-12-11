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

package se.uu.ub.cora.storage;

import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.Iterator;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;
import se.uu.ub.cora.spider.record.storage.RecordStorage;
import se.uu.ub.cora.storage.testdata.DataCreator;
import se.uu.ub.cora.storage.testdata.TestDataRecordInMemoryStorage;

public class RecordStorageInMemoryListTest {
	private RecordStorage recordStorage;
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	DataGroup emptyFilter = DataGroup.withNameInData("filter");
	private String dataDivider = "cora";

	@BeforeMethod
	public void beforeMethod() {
		recordStorage = new RecordStorageInMemory();
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", recordTypeRecordType,
				DataCreator.createEmptyCollectedData(), emptyLinkList, "cora");
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testListWithFilterButNoDataForTheType() {
		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		recordStorage.readList("place", filter);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithEmptyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm();
		createPlaceInStorageWithStockholmStorageTerm();

		Collection<DataGroup> readList = recordStorage.readList("place", emptyFilter);

		assertEquals(readList.size(), 2);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonMatchingFilter() {
		createPlaceInStorageWithUppsalaStorageTerm();
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"NOT_UPPSALA");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter);
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonExisitingKeyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm();
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "NOT_placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter);
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm();
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter);
		assertEquals(readList.size(), 1);
		DataGroup first = readList.iterator().next();
		assertEquals(
				first.getFirstGroupWithNameInData("recordInfo").getFirstAtomicValueWithNameInData("id"),
				"place:0001");
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilter2() {
		createPlaceInStorageWithUppsalaStorageTerm();
		createPlaceInStorageWithStockholmStorageTerm();
		createPlaceInStorageWithUppsalaStorageAndStockholmTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter);
		assertEquals(readList.size(), 2);
		Iterator<DataGroup> listIterator = readList.iterator();
		DataGroup first = listIterator.next();
		assertEquals(
				first.getFirstGroupWithNameInData("recordInfo").getFirstAtomicValueWithNameInData("id"),
				"place:0001");
		DataGroup second = listIterator.next();
		assertEquals(
				second.getFirstGroupWithNameInData("recordInfo").getFirstAtomicValueWithNameInData("id"),
				"place:0003");
	}

	private void createPlaceInStorageWithUppsalaStorageTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0001");

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place", "place:0001");
		DataGroup collectStorageTerm = DataGroup.withNameInData("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("place", "place:0001", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	private void createPlaceInStorageWithStockholmStorageTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0002");

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place", "place:0002");
		DataGroup collectStorageTerm = DataGroup.withNameInData("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Stockholm", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("place", "place:0002", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	private void createPlaceInStorageWithUppsalaStorageAndStockholmTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0003");

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place", "place:0003");
		DataGroup collectStorageTerm = DataGroup.withNameInData("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);
		DataGroup collectedDataTerm2 = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Stockholm", "placeName");
		collectStorageTerm.addChild(collectedDataTerm2);

		recordStorage.create("place", "place:0003", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadRecordListNotFound() {
		String recordType = "place_NOT_FOUND";
		recordStorage.readList(recordType, emptyFilter);
	}

	@Test
	public void testReadRecordList() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();
		String recordType = "place";
		Collection<DataGroup> recordList = recordStorage.readList(recordType, emptyFilter);
		assertEquals(recordList.iterator().next().getNameInData(), "authority");
	}

	@Test
	public void testReadAbstractRecordList() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		String recordType = "binary";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType, emptyFilter);
		assertEquals(recordList.size(), 3);
	}

	@Test
	public void testAbstractListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "id", "image:0001");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readAbstractList("binary", filter);
		assertEquals(readList.size(), 1);
		DataGroup first = readList.iterator().next();
		assertEquals(
				first.getFirstGroupWithNameInData("recordInfo").getFirstAtomicValueWithNameInData("id"),
				"image:0001");
	}

	private void createImageRecords() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0001");
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("image", "image:0001");
		DataGroup collectStorageTerm = DataGroup.withNameInData("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1", "idStorageTerm",
						"image:0001", "id");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("image", "image:0001", dataGroup, collectedData, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0002");
		dataGroup2.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));

		DataGroup collectedData2 = DataCreator.createCollectedDataWithTypeAndId("image", "image:0002");
		DataGroup collectStorageTerm2 = DataGroup.withNameInData("storage");
		collectedData2.addChild(collectStorageTerm2);

		DataGroup collectedDataTerm2 = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1", "IdStorageTerm",
						"image:0002", "id");
		collectStorageTerm2.addChild(collectedDataTerm2);

		recordStorage.create("image", "image:0002", dataGroup2, collectedData2, emptyLinkList,
				dataDivider);
	}

	private void createGenericBinaryRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"genericBinary", "genericBinary:0001");
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("childId", "childValue"));
		recordStorage.create("genericBinary", "genericBinary:0001", dataGroup,
				DataCreator.createEmptyCollectedData(), emptyLinkList, dataDivider);
	}

	@Test
	public void testReadAbstractRecordListOneImplementingChildHasNoRecords() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		// create no records of genericBinary

		String recordType = "binary";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType, emptyFilter);
		assertEquals(recordList.size(), 2);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadAbstractRecordListNoImplementingChildHasNoRecords() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		// create no records

		String recordType = "binary";
		recordStorage.readAbstractList(recordType, emptyFilter);
	}

}
