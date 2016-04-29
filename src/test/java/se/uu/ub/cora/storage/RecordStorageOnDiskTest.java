/*
 * Copyright 2016 Olov McKie
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
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.storage.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.storage.testdata.DataCreator;

public class RecordStorageOnDiskTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String FROM_RECORD_ID = "fromRecordId";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private DataGroup emptyLinkList = DataCreator.createLinkList();

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles();
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
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);
		String expectedRecordJson = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		assertEquals(readJsonFileFromDisk("place.json"), expectedRecordJson);

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"authority", "place", "place:0001");
	}

	private String readJsonFileFromDisk(String fileName) throws IOException {
		Path path = Paths.get(basePath, fileName);
		BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
		String line = null;
		String json = "";
		while ((line = reader.readLine()) != null) {
			json += line;
		}
		reader.close();
		return json;
	}

	@Test
	public void testRecordWithLinks() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, linkListWithTwoLinks);
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);
		String expectedRecordJson = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		assertEquals(readJsonFileFromDisk("place.json"), expectedRecordJson);

		String expectedLinkListJson = "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]" + ",\"name\":\"to\"}]"
				+ ",\"name\":\"recordToRecordLink\"}" + ",{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
				+ ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		Path path = Paths.get(basePath, "linkLists.json");
		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk("linkLists.json"), expectedLinkListJson);

		String expectedIncomingLinksJson = "{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"fromRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"place:0001\"}],\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"toRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"toRecordId2\"}],\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"list\"}],\"name\":\"place:0001\"}],\"name\":\"fromRecordType\"}]"
				+ ",\"name\":\"toRecordId2\"},{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"toRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"toRecordId\"}],\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}],\"name\":\"list\"}]"
				+ ",\"name\":\"place:0001\"}],\"name\":\"fromRecordType\"}]"
				+ ",\"name\":\"toRecordId\"}],\"name\":\"toRecordType\"}]"
				+ ",\"name\":\"incomingLinks\"}";
		Path path2 = Paths.get(basePath, "incomingLinks.json");
		assertTrue(Files.exists(path2));
		assertEquals(readJsonFileFromDisk("incomingLinks.json"), expectedIncomingLinksJson);
	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	@Test
	public void testInitWithFileOnDiskNoLinksOnDisk() {
		writePlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		assertJsonEqualDataGroup(linkListPlace, emptyLinkList);
	}

	private void assertJsonEqualDataGroup(DataGroup dataGroupActual, DataGroup dataGroupExpected) {
		assertEquals(convertDataGroupToJsonString(dataGroupActual),
				convertDataGroupToJsonString(dataGroupExpected));
	}

	private void writePlaceFileToDisk() {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = "place.json";
		writeFileToDisk(json, fileName);
	}

	private void writeFileToDisk(String json, String fileName) {
		Path path = FileSystems.getDefault().getPath(basePath, fileName);
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(),
					StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
			writer.close();
		} catch (IOException e) {
		}
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
	public void testInitPlaceAndPersonFileOnDisk() {
		writePlaceFileToDisk();
		writePersonFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), "authority");
		DataGroup dataGroupPersonOut = recordStorage.read("person", "person:0001");
		assertEquals(dataGroupPersonOut.getNameInData(), "authority");
	}

	private void writePersonFileToDisk() {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"person\"}" + ",{\"name\":\"id\",\"value\":\"person:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = "person.json";
		writeFileToDisk(json, fileName);
	}

	@Test
	public void testUpdate() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);

		dataGroup.addChild(DataAtomic.withNameInDataAndValue("someNameInData", "someValue"));
		recordStorage.update("place", "place:0001", dataGroup, emptyLinkList);
		String json = "{\"children\":[{\"children\":[{\"children\":["
				+ "{\"name\":\"type\",\"value\":\"place\"},"
				+ "{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"},{\"name\":\"someNameInData\",\"value\":\"someValue\"}]"
				+ ",\"name\":\"authority\"}],\"name\":\"recordList\"}";

		assertEquals(readJsonFileFromDisk("place.json"), json);
	}

	@Test
	public void testDelete() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		Path path = Paths.get(basePath, "place.json");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeleteRemoveOneRecord() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		Path path = Paths.get(basePath, "place.json");
		String expectedRecordJsonOneRecord = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		assertEquals(readJsonFileFromDisk("place.json"), expectedRecordJsonOneRecord);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		recordStorage.create("place", "place:0002", dataGroup2, emptyLinkList);
		String expectedRecordJsonTwoRecords = "{\"children\":[{\"children\":[{\"children\":["
				+ "{\"name\":\"type\",\"value\":\"place\"},"
				+ "{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}"
				+ ",{\"children\":[{\"children\":[{\"name\":\"type\",\"value\":\"place\"}"
				+ ",{\"name\":\"id\",\"value\":\"place:0002\"}],\"name\":\"recordInfo\"}]"
				+ ",\"name\":\"authority\"}],\"name\":\"recordList\"}";

		assertEquals(readJsonFileFromDisk("place.json"), expectedRecordJsonTwoRecords);

		recordStorage.deleteByTypeAndId("place", "place:0002");
		assertEquals(readJsonFileFromDisk("place.json"), expectedRecordJsonOneRecord);
	}

	@Test
	public void testDeleteFileOnDiskRemovedWhenNoRecordsLeft() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		Path path = Paths.get(basePath, "place.json");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testInitWithFileOnDiskLinksOnDisk() {
		writePlaceFileToDisk();
		writePlaceLinksFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		String expectedLinkListJson = "{\"children\":[" + "{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]" + ",\"name\":\"to\"}]"
				+ ",\"name\":\"recordToRecordLink\"}" + ",{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"collectedDataLinks\"}";
		assertEquals(convertDataGroupToJsonString(linkListPlace), expectedLinkListJson);

		DataGroup dataGroupTo = createDataGroupWithRecordInfo();
		recordStorage.create("toRecordType", "toRecordId", dataGroupTo, emptyLinkList);
		Collection<DataGroup> incomingLinksTo = recordStorage
				.generateLinkCollectionPointingToRecord("toRecordType", "toRecordId");

		assertEquals(incomingLinksTo.size(), 1);
	}

	private void writePlaceLinksFileToDisk() {
		String expectedLinkListJson = "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]" + ",\"name\":\"to\"}]"
				+ ",\"name\":\"recordToRecordLink\"}" + ",{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
				+ ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		writeFileToDisk(expectedLinkListJson, "linkLists.json");

		String expectedIncomingLinksJson = "{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"fromRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"place:0001\"}],\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"toRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"toRecordId2\"}],\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"list\"}],\"name\":\"place:0001\"}],\"name\":\"fromRecordType\"}]"
				+ ",\"name\":\"toRecordId2\"},{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":[{\"name\":\"linkedRecordType\""
				+ ",\"value\":\"toRecordType\"},{\"name\":\"linkedRecordId\""
				+ ",\"value\":\"toRecordId\"}],\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}],\"name\":\"list\"}]"
				+ ",\"name\":\"place:0001\"}],\"name\":\"fromRecordType\"}]"
				+ ",\"name\":\"toRecordId\"}],\"name\":\"toRecordType\"}]"
				+ ",\"name\":\"incomingLinks\"}";
		writeFileToDisk(expectedIncomingLinksJson, "incomingLinks.json");
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testCreateMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		removeTempFiles();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testUpdateMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		removeTempFiles();
		recordStorage.update("place", "place:0001", dataGroup, emptyLinkList);

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDeleteMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		removeTempFiles();
		recordStorage.deleteByTypeAndId("place", "place:0001");

	}
}
