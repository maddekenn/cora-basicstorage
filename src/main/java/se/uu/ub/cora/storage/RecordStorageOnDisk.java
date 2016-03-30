package se.uu.ub.cora.storage;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordStorage;

public class RecordStorageOnDisk implements RecordStorage {

	public static RecordStorageOnDisk createRecordStorageOnDisk() {
		return new RecordStorageOnDisk();
	}

	@Override
	public DataGroup read(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void create(String type, String id, DataGroup record, DataGroup linkList) {
		// TODO Auto-generated method stub
//		Path path = FileSystems.getDefault().getPath(".", name);
	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void update(String type, String id, DataGroup record, DataGroup linkList) {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<DataGroup> readList(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataGroup readLinkList(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

}
