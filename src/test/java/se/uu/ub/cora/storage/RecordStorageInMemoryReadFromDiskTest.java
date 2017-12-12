/*
 * Copyright 2016 Uppsala University Library
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
import static org.testng.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.storage.testdata.DataCreator;

public class RecordStorageInMemoryReadFromDiskTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	private RecordStorageOnDisk recordStorage;
	DataGroup emptyCollectedData = DataCreator.createEmptyCollectedData();

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles();
		setUpData();
	}

	private void deleteFiles() throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(basePath));
		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void setUpData() {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", placeRecordType, emptyCollectedData, emptyLinkList,
				"cora");
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, emptyCollectedData,
				emptyLinkList, "cora");
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles();
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void testInitNoFilesOnDisk() throws IOException {

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"authority", "place", "place:0001");
	}

	@Test
	public void testRecordWithLinks() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));

		Path path2 = Paths.get(basePath, "incomingLinks.json");
		assertFalse(Files.exists(path2));
	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	private void assertJsonEqualDataGroup(DataGroup dataGroupActual, DataGroup dataGroupExpected) {
		assertEquals(convertDataGroupToJsonString(dataGroupActual),
				convertDataGroupToJsonString(dataGroupExpected));
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = convertDataGroupToJson(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter convertDataGroupToJson(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	@Test
	public void testUpdate() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");

		dataGroup.addChild(DataAtomic.withNameInDataAndValue("someNameInData", "someValue"));
		recordStorage.update("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));

		Path path2 = Paths.get(basePath, "incomingLinks.json");
		assertFalse(Files.exists(path2));
	}

}
