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
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;
import se.uu.ub.cora.spider.record.storage.RecordStorage;
import se.uu.ub.cora.storage.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverter;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.storage.data.converter.JsonToDataConverterFactoryImp;

public class RecordStorageOnDisk extends RecordStorageInMemory implements RecordStorage, MetadataStorage {
	private String basePath;

	public static RecordStorageOnDisk createRecordStorageOnDiskWithBasePath(String basePath) {
		return new RecordStorageOnDisk(basePath);
	}

	private RecordStorageOnDisk(String basePath) {
		this.basePath = basePath;
		readStoredData();
	}

	private void readStoredData() {
		try {
			Stream<Path> list = Files.list(Paths.get(basePath));
			list.forEach(p -> readFile(p));
			list.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readFile(Path path) {
		try {
			BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
			String line = null;
			String json = "";
			while ((line = reader.readLine()) != null) {
				json += line;
			}
			String fileName = path.getFileName().toString();
			String recordType = fileName.substring(0, fileName.length() - 5);
			DataGroup recordList = convertJsonStringToDataGroup(json);
			ensureStorageExistsForRecordType(recordType);

			List<DataElement> records = recordList.getChildren();

			for (DataElement dataElement : records) {
				DataGroup record = (DataGroup) dataElement;

				DataGroup recordInfo = record.getFirstGroupWithNameInData("recordInfo");
				String recordId = recordInfo.getFirstAtomicValueWithNameInData("id");

				storeRecordByRecordTypeAndRecordId(recordType, recordId, record);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public RecordStorageOnDisk(Map<String, Map<String, DataGroup>> records) {
		throwErrorIfConstructorArgumentIsNull(records);
		this.records = records;
	}

	private void throwErrorIfConstructorArgumentIsNull(Map<String, Map<String, DataGroup>> records) {
		if (null == records) {
			throw new IllegalArgumentException("Records must not be null");
		}
	}

	private DataGroup convertJsonStringToDataGroup(String jsonRecord) {
		JsonParser jsonParser = new OrgJsonParser();
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverterFactory jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverter jsonToDataConverter = jsonToDataConverterFactory.createForJsonObject(jsonValue);
		DataPart dataPart = jsonToDataConverter.toInstance();
		return (DataGroup) dataPart;
	}

	@Override
	public void create(String recordType, String recordId, DataGroup record, DataGroup linkList) {
		super.create(recordType, recordId, record, linkList);

		Path path = FileSystems.getDefault().getPath(basePath, recordType + ".json");

		DataGroup recordList = DataGroup.withNameInData("recordList");
		Collection<DataGroup> readList = readList(recordType);
		for (DataElement dataElement : readList) {
			recordList.addChild(dataElement);
		}
		String json = convertDataRecordToJsonString(recordList);
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// throw new Exception();
			throw new RecordNotFoundException("No records exists with recordType: " + recordType);
		}
	}

	private String convertDataRecordToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = convertDataGroupToJson(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter convertDataGroupToJson(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

}
