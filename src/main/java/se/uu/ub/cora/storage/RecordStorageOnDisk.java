package se.uu.ub.cora.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

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
	}

	@Override
	public DataGroup read(String type, String id) {
		Path path = FileSystems.getDefault().getPath(basePath, type + ".json");
		try {
			BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
			String line = null;
			String json = "";
			while ((line = reader.readLine()) != null) {
				json += line;
			}
			return convertJsonStringToDataGroup(json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
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
	public void create(String type, String id, DataGroup record, DataGroup linkList) {
		Path path = FileSystems.getDefault().getPath(basePath, type + ".json");
		String json = convertDataRecordToJsonString(record);
		String content = "";
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// throw new Exception();
			throw new RecordNotFoundException("No records exists with recordType: " + type);
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
