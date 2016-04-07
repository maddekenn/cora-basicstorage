package se.uu.ub.cora.storage;

import static org.testng.Assert.assertEquals;

import java.io.File;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.storage.testdata.DataCreator;

public class RecordStorageOnDiskTest {
	private String basePath = "/tmp/recordStorageOnDisk/";

	@BeforeTest
	public void makeSureBasePathExistsAndIsEmpty() {
		File dir = new File(basePath);
		dir.mkdir();
	}

	@Test
	public void testInit() {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "0001Place", dataGroup, emptyLinkList);
		DataGroup dataGroupOut = recordStorage.read("place", "0001Place");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData", "place",
				"place:0001");
	}
}
