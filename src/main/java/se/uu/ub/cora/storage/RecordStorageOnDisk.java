package se.uu.ub.cora.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import se.uu.ub.cora.bookkeeper.data.DataElement;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.DataPart;
import se.uu.ub.cora.bookkeeper.storage.MetadataStorage;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.spider.record.storage.RecordStorage;
import se.uu.ub.cora.storage.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverter;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverterFactoryImp;

public class RecordStorageOnDisk extends RecordStorageInMemory
		implements RecordStorage, MetadataStorage {
	private String basePath;

	public static RecordStorageOnDisk createRecordStorageOnDiskWithBasePath(String basePath) {
		return new RecordStorageOnDisk(basePath);
	}

	private RecordStorageOnDisk(String basePath) {
		this.basePath = basePath;
		tryToReadStoredDataFromDisk();
	}

	private void tryToReadStoredDataFromDisk() {
		try {
			readStoredDataFromDisk();
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not read files from disk on init");
		}
	}

	private void readStoredDataFromDisk() throws IOException {
		Stream<Path> list = Files.list(Paths.get(basePath));
		Iterator<Path> iterator = list.iterator();
		while (iterator.hasNext()) {
			Path p = iterator.next();
			readFile(p);
		}
		list.close();
	}

	private void readFile(Path path) throws IOException {
		BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
		String line = null;
		String json = "";
		while ((line = reader.readLine()) != null) {
			json += line;
		}
		String fileName = path.getFileName().toString();
		String recordTypeName = fileName.substring(0, fileName.length() - 5);

		DataGroup recordList = convertJsonStringToDataGroup(json);
		if (fileName.equals("linkLists.json")) {
			List<DataElement> recordTypes = recordList.getChildren();

			for (DataElement typesElement : recordTypes) {
				DataGroup recordType = (DataGroup) typesElement;
				recordTypeName = recordType.getNameInData();
				ensureStorageExistsForRecordType(recordTypeName);

				List<DataElement> records = recordType.getChildren();
				for (DataElement recordElement : records) {
					DataGroup record = (DataGroup) recordElement;
					String recordId = record.getNameInData();
					DataGroup collectedDataLinks = (DataGroup) record
							.getFirstChildWithNameInData("collectedDataLinks");
					storeLinks(recordTypeName, recordId, collectedDataLinks);
				}
			}

		} else if (fileName.equals("incomingLinks.json")) {

		} else {
			ensureStorageExistsForRecordType(recordTypeName);

			List<DataElement> records = recordList.getChildren();

			for (DataElement dataElement : records) {
				DataGroup record = (DataGroup) dataElement;

				DataGroup recordInfo = record.getFirstGroupWithNameInData("recordInfo");
				String recordId = recordInfo.getFirstAtomicValueWithNameInData("id");

				storeRecordByRecordTypeAndRecordId(recordTypeName, recordId, record);
			}
		}
	}

	private DataGroup convertJsonStringToDataGroup(String jsonRecord) {
		JsonParser jsonParser = new OrgJsonParser();
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverterFactory jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverter jsonToDataConverter = jsonToDataConverterFactory
				.createForJsonObject(jsonValue);
		DataPart dataPart = jsonToDataConverter.toInstance();
		return (DataGroup) dataPart;
	}

	@Override
	public void create(String recordType, String recordId, DataGroup record, DataGroup linkList) {
		super.create(recordType, recordId, record, linkList);
		writeDataToDisk(recordType);
	}

	private void writeDataToDisk(String recordType) {
		writeRecordsToDisk(recordType);
		writeLinkListToDisk();
		writeIncomingLinksToDisk();
	}

	private void writeRecordsToDisk(String recordType) {
		Path path = FileSystems.getDefault().getPath(basePath, recordType + ".json");
		if (recordsExistForRecordType(recordType)) {
			Collection<DataGroup> readList = readList(recordType);

			DataGroup recordList = DataGroup.withNameInData("recordList");
			for (DataElement dataElement : readList) {
				recordList.addChild(dataElement);
			}
			writeDataGroupToDiskAsJson(path, recordList);
			// }
		} else {
			try {
				Files.delete(path);
			} catch (IOException e) {
				throw DataStorageException.withMessage("can not read files from disk on init");
			}
		}
	}

	private void writeDataGroupToDiskAsJson(Path path, DataGroup dataGroup) {
		String json = convertDataGroupToJsonString(dataGroup);
		BufferedWriter writer;
		try {
			if (Files.exists(path)) {
				Files.delete(path);
			}
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(),
					StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not read files from disk on init");
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = createDataGroupToJsonConvert(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter createDataGroupToJsonConvert(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	private void writeLinkListToDisk() {
		Path path = FileSystems.getDefault().getPath(basePath, "linkLists.json");

		boolean writeToFile = false;
		DataGroup linkListsGroup = DataGroup.withNameInData("linkLists");
		for (String recordTypeKey : linkLists.keySet()) {
			DataGroup recordTypeGroup = DataGroup.withNameInData(recordTypeKey);
			linkListsGroup.addChild(recordTypeGroup);
			Map<String, DataGroup> recordGroupMap = linkLists.get(recordTypeKey);
			for (String recordIdKey : recordGroupMap.keySet()) {
				DataGroup recordIdGroup = DataGroup.withNameInData(recordIdKey);
				recordTypeGroup.addChild(recordIdGroup);
				recordIdGroup.addChild(recordGroupMap.get(recordIdKey));
				writeToFile = true;
			}
		}
		if (writeToFile) {
			writeDataGroupToDiskAsJson(path, linkListsGroup);
		}
	}

	private void writeIncomingLinksToDisk() {
		Path path = FileSystems.getDefault().getPath(basePath, "incomingLinks.json");

		boolean writeToFile = false;
		DataGroup linkListsGroup = DataGroup.withNameInData("incomingLinks");

		for (String recordTypeToKey : incomingLinks.keySet()) {
			DataGroup recordTypeToGroup = DataGroup.withNameInData(recordTypeToKey);
			linkListsGroup.addChild(recordTypeToGroup);
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap = incomingLinks
					.get(recordTypeToKey);

			for (String recordIdKey : recordGroupMap.keySet()) {
				DataGroup recordIdGroup = DataGroup.withNameInData(recordIdKey);
				recordTypeToGroup.addChild(recordIdGroup);
				Map<String, Map<String, List<DataGroup>>> map2 = recordGroupMap.get(recordIdKey);

				for (String string2 : map2.keySet()) {
					DataGroup recordIdGroup2 = DataGroup.withNameInData(string2);
					recordIdGroup.addChild(recordIdGroup2);
					Map<String, List<DataGroup>> group2 = map2.get(string2);

					for (String string3 : group2.keySet()) {
						DataGroup recordIdGroup3 = DataGroup.withNameInData(string3);
						recordIdGroup2.addChild(recordIdGroup3);
						List<DataGroup> list = group2.get(string3);

						for (DataGroup dataGroup5 : list) {
							DataGroup recordIdGroup4 = DataGroup.withNameInData("list");
							recordIdGroup3.addChild(recordIdGroup4);
							recordIdGroup4.addChild(dataGroup5);
							writeToFile = true;
						}
					}
				}
			}
		}
		if (writeToFile) {
			writeDataGroupToDiskAsJson(path, linkListsGroup);
		}
	}

	@Override
	public void update(String recordType, String recordId, DataGroup record, DataGroup linkList) {
		super.update(recordType, recordId, record, linkList);
		writeDataToDisk(recordType);
	}

	@Override
	public void deleteByTypeAndId(String recordType, String recordId) {
		super.deleteByTypeAndId(recordType, recordId);
		writeDataToDisk(recordType);
	}
}
