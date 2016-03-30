package se.uu.ub.cora.storage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.json.parser.JsonArray;

public class RecordStorageOnDiskTest {

	@Test
	public void testInit() {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk.createRecordStorageOnDisk();
//		@Test
//		public void testCreateRead() {

//			DataGroup dataGroup = createDataGroupWithRecordInfo();
//			DataGroup dataGroup = DataGroup.withNameInData("someNameInData");
//
//			recordStorage.create("place", "0001Place", dataGroup, emptyLinkList);
//			DataGroup dataGroupOut = recordStorage.read("place", "0001Place");
//			assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());
//		}
		
	}
}
