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
	private String basePath = "/tmp/recordStorageOnDisk/";
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
		deleteFiles();
		File dir = new File(basePath);
		dir.delete();
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
		return json;

	}

	@Test
	public void testInitNoFilesOnDiskWithLinks() throws IOException {
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

		// String linkListJson = "{\"children\":[{\"children\":[{\"children\":["
		// + "{\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
		// + ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		// String linkListJson = "{\"children\":[{\"children\":[{\"children\":["
		// +
		// "{\"name\":\"collectedDataLinks\",\"children\":[{}]}],\"name\":\"place:0001\"}]"
		// + ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		// writeFileToDisk(linkListJson, "linkLists.json");
	}

	private void writeFileToDisk(String json, String fileName) {
		Path path = FileSystems.getDefault().getPath(basePath, fileName);
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(),
					StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
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
	public void testUpdatePlaceFileOnDisk() throws IOException {
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
	public void testInitWithFileOnDiskLinksOnDisk() {
		writePlaceFileToDisk();
		writePlaceLinksFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		assertJsonEqualDataGroup(linkListPlace, emptyLinkList);
	}

	private void writePlaceLinksFileToDisk() {
		// String json =
		// "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
		// + ",\"value\":\"place\"}" +
		// ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
		// +
		// ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";
		//
		// String fileName = "place.json";
		// writeFileToDisk(json, fileName);

		// String linkListJson = "{\"children\":[{\"children\":[{\"children\":["
		// + "{\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
		// + ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		// String linkListJson = "{\"children\":[{\"children\":[{\"children\":["
		// +
		// "{\"name\":\"collectedDataLinks\",\"children\":[{}]}],\"name\":\"place:0001\"}]"
		// + ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		// DataGroup linkListWithTwoLinks =
		// createLinkListWithTwoLinks("place:0001");
		// writeFileToDisk(convertDataGroupToJsonString(linkListWithTwoLinks),
		// "linkLists.json");

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

}
