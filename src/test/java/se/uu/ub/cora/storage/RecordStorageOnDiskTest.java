package se.uu.ub.cora.storage;

import static org.testng.Assert.assertEquals;

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

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.storage.testdata.DataCreator;

public class RecordStorageOnDiskTest {
	private String basePath = "/tmp/recordStorageOnDisk/";

	@BeforeTest
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

	@AfterTest
	public void removeTempFiles() throws IOException {
		deleteFiles();
		File dir = new File(basePath);
		dir.delete();
	}

	@Test
	public void testInit() {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyLinkList);
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority", "place",
				"place:0001");
	}

	@Test
	public void testInitPlaceFileOnDisk() {
		writePlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), "authority");
	}

	private void writePlaceFileToDisk() {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\"" + ",\"value\":\"place\"}"
				+ ",{\"name\":\"id\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = "place.json";
		writeFileToDisk(json, fileName);
	}

	private void writeFileToDisk(String json, String fileName) {
		Path path = FileSystems.getDefault().getPath(basePath, fileName);
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
		} catch (IOException e) {
		}
	}

	@Test
	public void testInitPlaceAndPersonFileOnDisk() {
		writePlaceFileToDisk();
		writePersonFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), "authority");
		DataGroup dataGroupPersonOut = recordStorage.read("person", "person:0001");
		assertEquals(dataGroupPersonOut.getNameInData(), "authority");
	}

	private void writePersonFileToDisk() {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\"" + ",\"value\":\"person\"}"
				+ ",{\"name\":\"id\",\"value\":\"person:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = "person.json";
		writeFileToDisk(json, fileName);
	}
}
