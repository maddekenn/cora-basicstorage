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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import se.uu.ub.cora.bookkeeper.data.DataElement;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.DataPart;
import se.uu.ub.cora.bookkeeper.storage.MetadataStorage;
import se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter;
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
	private static final String JSON_FILE_END = ".json";
	private String basePath;

	public static RecordStorageOnDisk createRecordStorageOnDiskWithBasePath(String basePath) {
		return new RecordStorageOnDisk(basePath);
	}

	protected RecordStorageOnDisk(String basePath) {
		this.basePath = basePath;
		tryToReadStoredDataFromDisk();
	}

	private void tryToReadStoredDataFromDisk() {
		try {
			readStoredDataFromDisk();
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not read files from disk on init" + e);
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
		StringBuilder jsonBuilder = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			jsonBuilder.append(line);
		}
		reader.close();
		String fileName = path.getFileName().toString();
		String fileNameTypePart = fileName.substring(0, fileName.lastIndexOf("_"));
		String dataDivider = fileName.substring(fileName.lastIndexOf("_") + 1,
				fileName.indexOf("."));
		DataGroup recordList = convertJsonStringToDataGroup(jsonBuilder.toString());
		if (fileNameTypePart.equals("linkLists")) {
			List<DataElement> recordTypes = recordList.getChildren();

			for (DataElement typesElement : recordTypes) {
				DataGroup recordType = (DataGroup) typesElement;
				String recordTypeName = recordType.getNameInData();
				ensureStorageExistsForRecordType(recordTypeName);

				List<DataElement> records = recordType.getChildren();
				for (DataElement recordElement : records) {
					DataGroup record = (DataGroup) recordElement;
					String recordId = record.getNameInData();
					DataGroup collectedDataLinks = (DataGroup) record
							.getFirstChildWithNameInData("collectedDataLinks");
					storeLinks(recordTypeName, recordId, collectedDataLinks, dataDivider);
				}
			}

		} else if (fileName.equals("incomingLinks_cora.json")) {
			// not read as information is recreated from linkLists.json
		} else {
			ensureStorageExistsForRecordType(fileNameTypePart);

			List<DataElement> records = recordList.getChildren();

			for (DataElement dataElement : records) {
				DataGroup record = (DataGroup) dataElement;

				DataGroup recordInfo = record.getFirstGroupWithNameInData("recordInfo");
				String recordId = recordInfo.getFirstAtomicValueWithNameInData("id");

				storeRecordByRecordTypeAndRecordId(fileNameTypePart, recordId, record, dataDivider);
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
	public void create(String recordType, String recordId, DataGroup record, DataGroup linkList,
			String dataDivider) {
		super.create(recordType, recordId, record, linkList, dataDivider);
		writeDataToDisk(recordType, dataDivider);
	}

	protected void writeDataToDisk(String recordType, String dataDivider) {
		writeRecordsToDisk(recordType, dataDivider);
		writeLinkListToDisk(dataDivider);
		writeIncomingLinksToDisk();
	}

	private void writeRecordsToDisk(String recordType, String dataDivider) {
		if (recordsExistForRecordType(recordType)) {
			writeRecordsToDiskWhereRecordTypeExists(recordType, dataDivider);
		} else {
			removeFileFromDisk(recordType, dataDivider);
		}
	}

	private void removeFileFromDisk(String recordType, String dataDivider) {
		String pathString2 = recordType + "_" + dataDivider + JSON_FILE_END;
		try {
			Path path = Paths.get(basePath, pathString2);
			Files.delete(path);
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not delete record files from disk" + e);
		}
	}

	private void writeRecordsToDiskWhereRecordTypeExists(String recordType, String dataDivider) {
		Map<String, DataGroup> recordLists = new HashMap<>();
		Map<String, DividerGroup> readList = records.get(recordType);

		for (Entry<String, DividerGroup> dividerEntry : readList.entrySet()) {
			DividerGroup dividerGroup = dividerEntry.getValue();
			String currentDataDivider = dividerGroup.dataDivider;
			DataGroup currentDataGroup = dividerGroup.dataGroup;
			if (!recordLists.containsKey(currentDataDivider)) {
				recordLists.put(currentDataDivider, DataGroup.withNameInData("recordList"));
			}
			recordLists.get(currentDataDivider).addChild(currentDataGroup);
		}
		for (Entry<String, DataGroup> recordListEntry : recordLists.entrySet()) {
			String pathString2 = recordType + "_" + recordListEntry.getKey() + JSON_FILE_END;
			writeDataGroupToDiskAsJson(pathString2, recordListEntry.getValue());
		}
		if (!recordLists.containsKey(dataDivider)) {
			removeFileFromDisk(recordType, dataDivider);
		}
	}

	private void writeDataGroupToDiskAsJson(String pathString, DataGroup dataGroup) {
		String json = convertDataGroupToJsonString(dataGroup);
		BufferedWriter writer;
		try {
			Path path = Paths.get(basePath, pathString);
			if (Files.exists(path)) {
				Files.delete(path);
			}
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(),
					StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not write files to disk" + e);
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = createDataGroupToJsonConvert(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter createDataGroupToJsonConvert(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	// Map<String, DataGroup> recordLists = new HashMap<>();
	// Map<String, DividerGroup> readList = records.get(recordType);

	// for (Entry<String, DividerGroup> dividerEntry : readList.entrySet()) {
	// DividerGroup dividerGroup = dividerEntry.getValue();
	// String currentDataDivider = dividerGroup.dataDivider;
	// DataGroup currentDataGroup = dividerGroup.dataGroup;
	// if (!recordLists.containsKey(currentDataDivider)) {
	// recordLists.put(currentDataDivider,
	// DataGroup.withNameInData("recordList"));
	// }
	// recordLists.get(currentDataDivider).addChild(currentDataGroup);
	// }
	// for (Entry<String, DataGroup> recordListEntry : recordLists.entrySet()) {
	// String pathString2 = recordType + "_" + recordListEntry.getKey() +
	// JSON_FILE_END;
	// writeDataGroupToDiskAsJson(pathString2, recordListEntry.getValue());
	// }
	// if (!recordLists.containsKey(dataDivider)) {
	// removeFileFromDisk(recordType, dataDivider);
	// }
	private void writeLinkListToDisk(String dataDivider) {
		String pathString = "linkLists_" + dataDivider + JSON_FILE_END;

		Map<String, DataGroup> linkListsGroups = new HashMap<>();

		boolean writeToFile = false;
		// DataGroup linkListsGroup = DataGroup.withNameInData("linkLists");
		boolean writeToFile1 = false;
		for (Entry<String, Map<String, DividerGroup>> recordType : linkLists.entrySet()) {

			// DataGroup recordTypeGroup =
			// DataGroup.withNameInData(recordType.getKey());
			Map<String, DividerGroup> recordGroupMap = recordType.getValue();
			boolean writeToFile2 = false;
			for (Entry<String, DividerGroup> recordEntry : recordGroupMap.entrySet()) {
				DividerGroup dataDividerGroup = recordEntry.getValue();
				String currentDataDivider = dataDividerGroup.dataDivider;
				if (!linkListsGroups.containsKey(currentDataDivider)) {
					linkListsGroups.put(currentDataDivider, DataGroup.withNameInData("linkLists"));
				}
				if (!linkListsGroups.get(currentDataDivider)
						.containsChildWithNameInData(recordType.getKey())) {
					DataGroup recordTypeGroup2 = DataGroup.withNameInData(recordType.getKey());
					linkListsGroups.get(currentDataDivider).addChild(recordTypeGroup2);
				}
				DataGroup recordTypeChild = (DataGroup) linkListsGroups.get(currentDataDivider)
						.getFirstChildWithNameInData(recordType.getKey());

				DataGroup recordIdGroup = DataGroup.withNameInData(recordEntry.getKey());
				// recordTypeGroup.addChild(recordIdGroup);
				recordTypeChild.addChild(recordIdGroup);
				recordIdGroup.addChild(dataDividerGroup.dataGroup);
				writeToFile2 = true;
			}
			// boolean currentWriteToFile =
			// addRecordIdsToGroupFromMap(recordTypeGroup,
			boolean currentWriteToFile = writeToFile2;
			if (currentWriteToFile) {
				// linkListsGroup.addChild(recordTypeGroup);
				writeToFile1 = true;
			}

		}
		// writeToFile = addRecordTypes(linkListsGroup);
		writeToFile = writeToFile1;
		// if (writeToFile) {
		// writeDataGroupToDiskAsJson(pathString, linkListsGroup);
		// }
		for (Entry<String, DataGroup> recordListEntry : linkListsGroups.entrySet()) {
			String pathString2 = "linkLists_" + recordListEntry.getKey() + JSON_FILE_END;
			writeDataGroupToDiskAsJson(pathString2, recordListEntry.getValue());
		}
		if (!linkListsGroups.containsKey(dataDivider)) {
			String pathString2 = "linkLists_" + dataDivider + JSON_FILE_END;
			Path path = Paths.get(basePath, pathString2);
			if (Files.exists(path)) {
				removeFileFromDisk("linkLists", dataDivider);
			}
		}

	}

	// private boolean addRecordTypes(DataGroup linkListsGroup) {
	// boolean writeToFile = false;
	// for (Entry<String, Map<String, DividerGroup>> recordType :
	// linkLists.entrySet()) {
	// DataGroup recordTypeGroup =
	// DataGroup.withNameInData(recordType.getKey());
	// Map<String, DividerGroup> recordGroupMap = recordType.getValue();
	// boolean currentWriteToFile = addRecordIdsToGroupFromMap(recordTypeGroup,
	// recordGroupMap);
	// if (currentWriteToFile) {
	// linkListsGroup.addChild(recordTypeGroup);
	// writeToFile = true;
	// }
	// }
	// return writeToFile;
	// }

	// private boolean addRecordIdsToGroupFromMap(DataGroup recordTypeGroup,
	// Map<String, DividerGroup> recordGroupMap) {
	// boolean writeToFile = false;
	// for (Entry<String, DividerGroup> recordEntry : recordGroupMap.entrySet())
	// {
	// DataGroup recordIdGroup = DataGroup.withNameInData(recordEntry.getKey());
	// recordTypeGroup.addChild(recordIdGroup);
	// recordIdGroup.addChild(recordEntry.getValue().dataGroup);
	// writeToFile = true;
	// }
	// return writeToFile;
	// }

	private void writeIncomingLinksToDisk() {
		String pathString = "incomingLinks_cora.json";

		DataGroup linkListsGroup = DataGroup.withNameInData("incomingLinks");
		boolean writeToFile = addLinksToLinkList(linkListsGroup);
		if (writeToFile) {
			writeDataGroupToDiskAsJson(pathString, linkListsGroup);
		}
	}

	private boolean addLinksToLinkList(DataGroup linkListsGroup) {
		return addToTypeToList(linkListsGroup);
	}

	private boolean addToTypeToList(DataGroup linkListsGroup) {
		boolean writeToFile = false;
		for (Entry<String, Map<String, Map<String, Map<String, List<DataGroup>>>>> recordTypeTo : incomingLinks
				.entrySet()) {
			DataGroup recordTypeToGroup = DataGroup.withNameInData(recordTypeTo.getKey());
			linkListsGroup.addChild(recordTypeToGroup);
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap = recordTypeTo
					.getValue();

			addToIdToList(recordTypeToGroup, recordGroupMap);
			writeToFile = true;
		}
		return writeToFile;
	}

	private void addToIdToList(DataGroup recordTypeToGroup,
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap) {
		for (Entry<String, Map<String, Map<String, List<DataGroup>>>> recordId : recordGroupMap
				.entrySet()) {
			DataGroup recordIdGroup = DataGroup.withNameInData(recordId.getKey());
			recordTypeToGroup.addChild(recordIdGroup);
			Map<String, Map<String, List<DataGroup>>> fromGroupMap = recordId.getValue();

			addFromType(recordIdGroup, fromGroupMap);
		}
	}

	private void addFromType(DataGroup recordIdGroup,
			Map<String, Map<String, List<DataGroup>>> fromGroupMap) {
		for (Entry<String, Map<String, List<DataGroup>>> fromGroup : fromGroupMap.entrySet()) {
			DataGroup fromTypeGroup = DataGroup.withNameInData(fromGroup.getKey());
			recordIdGroup.addChild(fromTypeGroup);
			Map<String, List<DataGroup>> fromIdGroup = fromGroup.getValue();

			addFromId(fromTypeGroup, fromIdGroup);
		}
	}

	private void addFromId(DataGroup fromTypeGroup, Map<String, List<DataGroup>> fromIdGroup) {
		for (Entry<String, List<DataGroup>> fromId : fromIdGroup.entrySet()) {
			DataGroup recordIdGroup3 = DataGroup.withNameInData(fromId.getKey());
			fromTypeGroup.addChild(recordIdGroup3);
			List<DataGroup> links = fromId.getValue();

			addLinks(recordIdGroup3, links);
		}
	}

	private void addLinks(DataGroup recordIdGroup3, List<DataGroup> links) {
		for (DataGroup link : links) {
			DataGroup recordIdGroup4 = DataGroup.withNameInData("list");
			recordIdGroup3.addChild(recordIdGroup4);
			recordIdGroup4.addChild(link);
		}
	}

	@Override
	public void update(String recordType, String recordId, DataGroup record, DataGroup linkList,
			String dataDivider) {
		String previousDataDivider = records.get(recordType).get(recordId).dataDivider;
		super.update(recordType, recordId, record, linkList, dataDivider);
		writeDataToDisk(recordType, previousDataDivider);
	}

	@Override
	public void deleteByTypeAndId(String recordType, String recordId) {
		String previousDataDivider = records.get(recordType).get(recordId).dataDivider;
		super.deleteByTypeAndId(recordType, recordId);
		writeDataToDisk(recordType, previousDataDivider);
	}
}
