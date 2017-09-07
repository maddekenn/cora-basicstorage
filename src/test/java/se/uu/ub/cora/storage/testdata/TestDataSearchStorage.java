/*
 * Copyright 2017 Uppsala University Library
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

package se.uu.ub.cora.storage.testdata;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.DataPart;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverter;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverterFactoryImp;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.storage.RecordStorageInMemory;
import se.uu.ub.cora.storage.RecordStorageOnDisk;

public class TestDataSearchStorage {

	public static RecordStorageInMemory createRecordStorageInMemoryWithTestData(String basePath) {
		RecordStorageOnDisk recordsOnDisk = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		addRecordType(recordsOnDisk);
		addRecordTypeRecordType(recordsOnDisk);
		addRecordTypeSearchTerm(recordsOnDisk);
		// addRecordTypeSystemTwoUser(recordsOnDisk);
		// addRecordTypeAppToken(recordsOnDisk);

		// addRecordTypeImage(recordsOnDisk);
		String searchTermJson = "{\"name\":\"searchTerm\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"someSearchTerm\"},{\"name\":\"type\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"recordType\"},{\"name\":\"linkedRecordId\",\"value\":\"searchTerm\"}]},{\"name\":\"createdBy\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"user\"},{\"name\":\"linkedRecordId\",\"value\":\"141414\"}]},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"cora\"}]}]},{\"name\":\"searchTermType\",\"value\":\"linkedData\"},{\"name\":\"searchFieldRef\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"metadata\"},{\"name\":\"linkedRecordId\",\"value\":\"refTextVar\"}]},{\"name\":\"indexType\",\"value\":\"indexTypeString\"}]}";
		DataGroup searchTerm = convertJsonStringToDataGroup(searchTermJson);
		recordsOnDisk.create("searchTerm", "someSearchTerm", searchTerm,
				DataGroup.withNameInData("collectedLinksList"), "systemOne");
		// String dummyUserJson1 =
		// "{\"name\":\"user\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"dummy1\"},{\"name\":\"type\",\"value\":\"systemOneUser\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"systemOne\"}]}]},{\"name\":\"userId\",\"value\":\"dummy@ub.uu.se\"},{\"name\":\"userFirstname\",\"value\":\"Dummy\"},{\"name\":\"userLastname\",\"value\":\"Dumsson\"},{\"name\":\"userRole\",\"children\":[{\"name\":\"userRole\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"permissionRole\"},{\"name\":\"linkedRecordId\",\"value\":\"nothing\"}]},{\"name\":\"userRoleRulePart\",\"children\":[{\"name\":\"permissionRulePart\",\"children\":[{\"name\":\"permissionRulePartValue\",\"value\":\"system.\",\"repeatId\":\"0\"}],\"attributes\":{\"type\":\"organisation\"}}]}],\"repeatId\":\"0\"},{\"name\":\"activeStatus\",\"value\":\"active\"},{\"name\":\"userAppTokenGroup\",\"children\":[{\"name\":\"appTokenLink\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"appToken\"},{\"name\":\"linkedRecordId\",\"value\":\"appToken1\"}]},{\"name\":\"note\",\"value\":\"My
		// phone\"}],\"repeatId\":\"1\"}]}";
		//
		// String appTokenJson1 =
		// "{\"name\":\"appToken\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"appToken1\"},{\"name\":\"type\",\"value\":\"appToken\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"cora\"}]}]},{\"name\":\"token\",\"value\":\"someSecretString\"}]}";
		// DataGroup appToken1 = convertJsonStringToDataGroup(appTokenJson1);
		// recordsOnDisk.create("appToken", "appToken1", appToken1,
		// DataGroup.withNameInData("collectedLinksList"), "cora");
		//
		// String dummyUserJson2 =
		// "{\"name\":\"user\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"dummy2\"},{\"name\":\"type\",\"value\":\"systemTwoUser\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"systemOne\"}]}]},{\"name\":\"userId\",\"value\":\"dummy@ub.uu.se\"},{\"name\":\"userFirstname\",\"value\":\"Dummy\"},{\"name\":\"userLastname\",\"value\":\"Dumsson\"},{\"name\":\"userRole\",\"children\":[{\"name\":\"userRole\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"permissionRole\"},{\"name\":\"linkedRecordId\",\"value\":\"nothing\"}]},{\"name\":\"userRoleRulePart\",\"children\":[{\"name\":\"permissionRulePart\",\"children\":[{\"name\":\"permissionRulePartValue\",\"value\":\"system.\",\"repeatId\":\"0\"}],\"attributes\":{\"type\":\"organisation\"}}]}],\"repeatId\":\"0\"},{\"name\":\"activeStatus\",\"value\":\"active\"},{\"name\":\"userAppTokenGroup\",\"children\":[{\"name\":\"appTokenLink\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"appToken\"},{\"name\":\"linkedRecordId\",\"value\":\"appToken2\"}]},{\"name\":\"note\",\"value\":\"My
		// phone\"}],\"repeatId\":\"1\"}]}";
		// DataGroup dummyUser2 = convertJsonStringToDataGroup(dummyUserJson2);
		// recordsOnDisk.create("systemTwoUser", "dummy2", dummyUser2,
		// DataGroup.withNameInData("collectedLinksList"), "systemOne");
		//
		// String appTokenJson2 =
		// "{\"name\":\"appToken\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"appToken2\"},{\"name\":\"type\",\"value\":\"appToken\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"cora\"}]}]},{\"name\":\"token\",\"value\":\"someOtherSecretString\"}]}";
		// DataGroup appToken2 = convertJsonStringToDataGroup(appTokenJson2);
		// recordsOnDisk.create("appToken", "appToken2", appToken2,
		// DataGroup.withNameInData("collectedLinksList"), "cora");
		//
		// String inactiveUserJson =
		// "{\"name\":\"user\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"inactiveUser\"},{\"name\":\"type\",\"value\":\"systemTwoUser\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"systemOne\"}]}]},{\"name\":\"userId\",\"value\":\"dummy@ub.uu.se\"},{\"name\":\"userFirstname\",\"value\":\"Dummy\"},{\"name\":\"userLastname\",\"value\":\"Dumsson\"},{\"name\":\"userRole\",\"children\":[{\"name\":\"userRole\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"permissionRole\"},{\"name\":\"linkedRecordId\",\"value\":\"nothing\"}]},{\"name\":\"userRoleRulePart\",\"children\":[{\"name\":\"permissionRulePart\",\"children\":[{\"name\":\"permissionRulePartValue\",\"value\":\"system.\",\"repeatId\":\"0\"}],\"attributes\":{\"type\":\"organisation\"}}]}],\"repeatId\":\"0\"},{\"name\":\"activeStatus\",\"value\":\"inactive\"},{\"name\":\"userAppTokenGroup\",\"children\":[{\"name\":\"appTokenLink\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"appToken\"},{\"name\":\"linkedRecordId\",\"value\":\"appTokenJson\"}]},{\"name\":\"note\",\"value\":\"My
		// phone\"}],\"repeatId\":\"1\"}]}";
		// DataGroup inactiveUser =
		// convertJsonStringToDataGroup(inactiveUserJson);
		// recordsOnDisk.create("systemTwoUser", "inactiveUser", inactiveUser,
		// DataGroup.withNameInData("collectedLinksList"), "systemTwo");

		// String appTokenJson3 =
		// "{\"name\":\"appToken\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"appTokenJson\"},{\"name\":\"type\",\"value\":\"appToken\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"cora\"}]}]},{\"name\":\"token\",\"value\":\"someOtherSecretString\"}]}";
		// DataGroup appTokenJson = convertJsonStringToDataGroup(appTokenJson3);
		// recordsOnDisk.create("appToken", "appTokenJson", appTokenJson,
		// DataGroup.withNameInData("collectedLinksList"), "cora");
		//
		// String noAppTokenUserJson =
		// "{\"name\":\"user\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"noAppTokenUser\"},{\"name\":\"type\",\"value\":\"systemTwoUser\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"systemOne\"}]}]},{\"name\":\"userId\",\"value\":\"dummy@ub.uu.se\"},{\"name\":\"userFirstname\",\"value\":\"Dummy\"},{\"name\":\"userLastname\",\"value\":\"Dumsson\"},{\"name\":\"userRole\",\"children\":[{\"name\":\"userRole\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"permissionRole\"},{\"name\":\"linkedRecordId\",\"value\":\"nothing\"}]},{\"name\":\"userRoleRulePart\",\"children\":[{\"name\":\"permissionRulePart\",\"children\":[{\"name\":\"permissionRulePartValue\",\"value\":\"system.\",\"repeatId\":\"0\"}],\"attributes\":{\"type\":\"organisation\"}}]}],\"repeatId\":\"0\"},{\"name\":\"activeStatus\",\"value\":\"inactive\"}]}";
		// DataGroup noAppTokenUser =
		// convertJsonStringToDataGroup(noAppTokenUserJson);
		// recordsOnDisk.create("systemTwoUser", "noAppTokenUser",
		// noAppTokenUser,
		// DataGroup.withNameInData("collectedLinksList"), "systemTwo");
		//
		// String guestUserJson =
		// "{\"name\":\"user\",\"children\":[{\"name\":\"recordInfo\",\"children\":[{\"name\":\"id\",\"value\":\"12345\"},{\"name\":\"type\",\"value\":\"systemOneUser\"},{\"name\":\"createdBy\",\"value\":\"131313\"},{\"name\":\"dataDivider\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"},{\"name\":\"linkedRecordId\",\"value\":\"systemOne\"}]}]},{\"name\":\"userId\",\"value\":\"dummy@ub.uu.se\"},{\"name\":\"userFirstname\",\"value\":\"Dummy\"},{\"name\":\"userLastname\",\"value\":\"Dumsson\"},{\"name\":\"userRole\",\"children\":[{\"name\":\"userRole\",\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"permissionRole\"},{\"name\":\"linkedRecordId\",\"value\":\"nothing\"}]},{\"name\":\"userRoleRulePart\",\"children\":[{\"name\":\"permissionRulePart\",\"children\":[{\"name\":\"permissionRulePartValue\",\"value\":\"system.\",\"repeatId\":\"0\"}],\"attributes\":{\"type\":\"organisation\"}}]}],\"repeatId\":\"0\"},{\"name\":\"activeStatus\",\"value\":\"inactive\"}]}";
		// DataGroup guestUser = convertJsonStringToDataGroup(guestUserJson);
		// recordsOnDisk.create("systemOneUser", "12345", guestUser,
		// DataGroup.withNameInData("collectedLinksList"), "systemTwo");

		return recordsOnDisk;
	}

	private static DataGroup convertJsonStringToDataGroup(String jsonRecord) {
		JsonParser jsonParser = new OrgJsonParser();
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverterFactory jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverter jsonToDataConverter = jsonToDataConverterFactory
				.createForJsonObject(jsonValue);
		DataPart dataPart = jsonToDataConverter.toInstance();
		return (DataGroup) dataPart;
	}

	private static void addRecordType(RecordStorageInMemory recordsInMemory) {
		String recordType = "recordType";
		DataGroup dataGroup = DataGroup.withNameInData(recordType);

		DataGroup recordInfo = DataCreator.createRecordInfoWithRecordTypeAndRecordId(recordType,
				"metadata");
		dataGroup.addChild(recordInfo);

		dataGroup.addChild(DataAtomic.withNameInDataAndValue("abstract", "false"));
		recordsInMemory.create(recordType, "metadata", dataGroup,
				DataGroup.withNameInData("collectedLinksList"), "cora");
	}

	private static void addRecordTypeRecordType(RecordStorageOnDisk recordsOnDisk) {
		String recordType = "recordType";
		DataGroup dataGroup = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordsOnDisk.create(recordType, "recordType", dataGroup,
				DataGroup.withNameInData("collectedLinksList"), "cora");
	}

	// private static void addRecordTypeImage(RecordStorageInMemory
	// recordsInMemory) {
	// String recordType = "recordType";
	// DataGroup dataGroup = DataCreator
	// .createRecordTypeWithIdAndUserSuppliedIdAndParentId("image", "true",
	// "binary");
	// recordsInMemory.create(recordType, "image", dataGroup,
	// DataGroup.withNameInData("collectedLinksList"), "cora");
	// }

	private static void addRecordTypeSearchTerm(RecordStorageInMemory recordsInMemory) {
		String recordType = "recordType";
		DataGroup dataGroup = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("searchTerm", "true", "false");
		recordsInMemory.create(recordType, "searchTerm", dataGroup,
				DataGroup.withNameInData("collectedLinksList"), "cora");
	}

	// private static void addRecordTypeSystemTwoUser(RecordStorageInMemory
	// recordsInMemory) {
	// String recordType = "recordType";
	// DataGroup dataGroup =
	// DataCreator.createRecordTypeWithIdAndUserSuppliedIdAndParentId(
	// "systemTwoUser", "true", "user");
	// recordsInMemory.create(recordType, "systemTwoUser", dataGroup,
	// DataGroup.withNameInData("collectedLinksList"), "cora");
	// }
	//
	// private static void addRecordTypeAppToken(RecordStorageInMemory
	// recordsInMemory) {
	// String recordType = "recordType";
	// DataGroup dataGroup = DataCreator
	// .createRecordTypeWithIdAndUserSuppliedIdAndAbstract("appToken", "false",
	// "false");
	// recordsInMemory.create(recordType, "appToken", dataGroup,
	// DataGroup.withNameInData("collectedLinksList"), "cora");
	//
	// }
}
