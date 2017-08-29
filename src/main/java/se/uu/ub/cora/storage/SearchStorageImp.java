package se.uu.ub.cora.storage;

import java.util.Map;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.searchstorage.SearchStorage;

public class SearchStorageImp implements SearchStorage {
	RecordStorageInMemoryReadFromDisk recordStorage;
	private final String basePath;

	public SearchStorageImp(Map<String, String> initInfo) {
		if (!initInfo.containsKey("storageOnDiskBasePath")) {
			throw new RuntimeException("initInfo must contain storageOnDiskBasePath");
		}
		basePath = initInfo.get("storageOnDiskBasePath");
		recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		// populateFromStorage();
	}

	@Override
	public DataGroup getSearchTerm(String searchTermId) {
		return recordStorage.read("searchTerm", searchTermId);
		// return null;
	}

}
