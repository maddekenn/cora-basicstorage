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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

public final class RecordStorageOnDisk extends RecordStorageInMemory
		implements RecordStorage, MetadataStorage {
	private static final int JSON_FILE_END = 5;
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
		String recordTypeName = fileName.substring(0, fileName.length() - JSON_FILE_END);

		DataGroup recordList = convertJsonStringToDataGroup(jsonBuilder.toString());
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
			// not read as information is recreated from linkLists.json
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
		String pathString = recordType + ".json";
		if (recordsExistForRecordType(recordType)) {
			Collection<DataGroup> readList = readList(recordType);

			DataGroup recordList = DataGroup.withNameInData("recordList");
			for (DataElement dataElement : readList) {
				recordList.addChild(dataElement);
			}
			writeDataGroupToDiskAsJson(pathString, recordList);
		} else {
			try {
				Path path = Paths.get(basePath, pathString);
				Files.delete(path);
			} catch (IOException e) {
				throw DataStorageException.withMessage("can not write record files to disk" + e);
			}
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
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	private void writeLinkListToDisk() {
		String pathString = "linkLists.json";

		boolean writeToFile = false;
		DataGroup linkListsGroup = DataGroup.withNameInData("linkLists");
		writeToFile = addRecordTypes(linkListsGroup);
		if (writeToFile) {
			writeDataGroupToDiskAsJson(pathString, linkListsGroup);
		}

	}

	private boolean addRecordTypes(DataGroup linkListsGroup) {
		boolean writeToFile = false;
		for (Entry<String, Map<String, DataGroup>> recordType : linkLists.entrySet()) {
			DataGroup recordTypeGroup = DataGroup.withNameInData(recordType.getKey());
			linkListsGroup.addChild(recordTypeGroup);
			Map<String, DataGroup> recordGroupMap = recordType.getValue();
			boolean currentWriteToFile = addRecordIds(recordTypeGroup, recordGroupMap);
			if (currentWriteToFile) {
				writeToFile = true;
			}
		}
		return writeToFile;
	}

	private boolean addRecordIds(DataGroup recordTypeGroup, Map<String, DataGroup> recordGroupMap) {
		boolean writeToFile = false;
		for (Entry<String, DataGroup> recordId : recordGroupMap.entrySet()) {
			DataGroup recordIdGroup = DataGroup.withNameInData(recordId.getKey());
			recordTypeGroup.addChild(recordIdGroup);
			recordIdGroup.addChild(recordId.getValue());
			writeToFile = true;
		}
		return writeToFile;
	}

	private void writeIncomingLinksToDisk() {
		String pathString = "incomingLinks.json";

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

			boolean currentWriteToFile = addToIdToList(recordTypeToGroup, recordGroupMap);
			if (currentWriteToFile) {
				writeToFile = true;
			}
		}
		return writeToFile;
	}

	private boolean addToIdToList(DataGroup recordTypeToGroup,
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap) {
		boolean writeToFile = false;
		for (Entry<String, Map<String, Map<String, List<DataGroup>>>> recordId : recordGroupMap
				.entrySet()) {
			DataGroup recordIdGroup = DataGroup.withNameInData(recordId.getKey());
			recordTypeToGroup.addChild(recordIdGroup);
			Map<String, Map<String, List<DataGroup>>> fromGroupMap = recordId.getValue();

			boolean currentWriteToFile = addFromType(recordIdGroup, fromGroupMap);
			if (currentWriteToFile) {
				writeToFile = true;
			}
		}
		return writeToFile;
	}

	private boolean addFromType(DataGroup recordIdGroup,
			Map<String, Map<String, List<DataGroup>>> fromGroupMap) {
		boolean writeToFile = false;
		for (Entry<String, Map<String, List<DataGroup>>> fromGroup : fromGroupMap.entrySet()) {
			DataGroup fromTypeGroup = DataGroup.withNameInData(fromGroup.getKey());
			recordIdGroup.addChild(fromTypeGroup);
			Map<String, List<DataGroup>> fromIdGroup = fromGroup.getValue();

			boolean currentWriteToFile = addFromId(fromTypeGroup, fromIdGroup);
			if (currentWriteToFile) {
				writeToFile = true;
			}
		}
		return writeToFile;
	}

	private boolean addFromId(DataGroup fromTypeGroup, Map<String, List<DataGroup>> fromIdGroup) {
		boolean writeToFile = false;
		for (Entry<String, List<DataGroup>> fromId : fromIdGroup.entrySet()) {
			DataGroup recordIdGroup3 = DataGroup.withNameInData(fromId.getKey());
			fromTypeGroup.addChild(recordIdGroup3);
			List<DataGroup> links = fromId.getValue();

			boolean currentWriteToFile = addLinks(recordIdGroup3, links);
			if (currentWriteToFile) {
				writeToFile = true;
			}
		}
		return writeToFile;
	}

	private boolean addLinks(DataGroup recordIdGroup3, List<DataGroup> links) {
		boolean writeToFile = false;
		for (DataGroup link : links) {
			DataGroup recordIdGroup4 = DataGroup.withNameInData("list");
			recordIdGroup3.addChild(recordIdGroup4);
			recordIdGroup4.addChild(link);
			writeToFile = true;
		}
		return writeToFile;
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
