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
		writeToFile = addRecordTypes(writeToFile, linkListsGroup);
		if (writeToFile) {
			writeDataGroupToDiskAsJson(path, linkListsGroup);
		}
	}

	private boolean addRecordTypes(boolean writeToFile, DataGroup linkListsGroup) {
		for (String recordTypeKey : linkLists.keySet()) {
			DataGroup recordTypeGroup = DataGroup.withNameInData(recordTypeKey);
			linkListsGroup.addChild(recordTypeGroup);
			Map<String, DataGroup> recordGroupMap = linkLists.get(recordTypeKey);
			writeToFile = addRecordIds(writeToFile, recordTypeGroup, recordGroupMap);
		}
		return writeToFile;
	}

	private boolean addRecordIds(boolean writeToFile, DataGroup recordTypeGroup,
			Map<String, DataGroup> recordGroupMap) {
		for (String recordIdKey : recordGroupMap.keySet()) {
			DataGroup recordIdGroup = DataGroup.withNameInData(recordIdKey);
			recordTypeGroup.addChild(recordIdGroup);
			recordIdGroup.addChild(recordGroupMap.get(recordIdKey));
			writeToFile = true;
		}
		return writeToFile;
	}

	private void writeIncomingLinksToDisk() {
		Path path = FileSystems.getDefault().getPath(basePath, "incomingLinks.json");

		DataGroup linkListsGroup = DataGroup.withNameInData("incomingLinks");
		boolean writeToFile = addLinksToLinkList(linkListsGroup);
		if (writeToFile) {
			writeDataGroupToDiskAsJson(path, linkListsGroup);
		}
	}

	private boolean addLinksToLinkList(DataGroup linkListsGroup) {
		boolean writeToFile = false;
		// TODO: refactor this
		writeToFile = addToTypeToList(linkListsGroup, writeToFile);
		return writeToFile;
	}

	private boolean addToTypeToList(DataGroup linkListsGroup, boolean writeToFile) {
		for (String recordTypeToKey : incomingLinks.keySet()) {
			DataGroup recordTypeToGroup = DataGroup.withNameInData(recordTypeToKey);
			linkListsGroup.addChild(recordTypeToGroup);
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap = incomingLinks
					.get(recordTypeToKey);

			writeToFile = addToIdToList(writeToFile, recordTypeToGroup, recordGroupMap);
		}
		return writeToFile;
	}

	private boolean addToIdToList(boolean writeToFile, DataGroup recordTypeToGroup,
			Map<String, Map<String, Map<String, List<DataGroup>>>> recordGroupMap) {
		for (String recordIdKey : recordGroupMap.keySet()) {
			DataGroup recordIdGroup = DataGroup.withNameInData(recordIdKey);
			recordTypeToGroup.addChild(recordIdGroup);
			Map<String, Map<String, List<DataGroup>>> fromGroupMap = recordGroupMap
					.get(recordIdKey);

			writeToFile = addFromType(writeToFile, recordIdGroup, fromGroupMap);
		}
		return writeToFile;
	}

	private boolean addFromType(boolean writeToFile, DataGroup recordIdGroup,
			Map<String, Map<String, List<DataGroup>>> fromGroupMap) {
		for (String fromGroupKey : fromGroupMap.keySet()) {
			DataGroup fromTypeGroup = DataGroup.withNameInData(fromGroupKey);
			recordIdGroup.addChild(fromTypeGroup);
			Map<String, List<DataGroup>> fromIdGroup = fromGroupMap.get(fromGroupKey);

			writeToFile = addFromId(writeToFile, fromTypeGroup, fromIdGroup);
		}
		return writeToFile;
	}

	private boolean addFromId(boolean writeToFile, DataGroup fromTypeGroup,
			Map<String, List<DataGroup>> fromIdGroup) {
		for (String fromIdKey : fromIdGroup.keySet()) {
			DataGroup recordIdGroup3 = DataGroup.withNameInData(fromIdKey);
			fromTypeGroup.addChild(recordIdGroup3);
			List<DataGroup> links = fromIdGroup.get(fromIdKey);

			writeToFile = addLinks(writeToFile, recordIdGroup3, links);
		}
		return writeToFile;
	}

	private boolean addLinks(boolean writeToFile, DataGroup recordIdGroup3, List<DataGroup> links) {
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
