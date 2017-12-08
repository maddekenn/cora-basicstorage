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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.storage.testdata.DataCreator;

public class RecordStorageOnDiskTest {
	private static final String PERSON_FILENAME = "person_cora.json";
	private static final String PLACE_FILENAME = "place_cora.json";
	private static final String COLLECTED_DATA_FILENAME = "collectedData_cora.json";
	private static final String LINK_LISTS_FILENAME = "linkLists_cora.json";
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	DataGroup emptyCollectedData = DataCreator.createEmptyCollectedData();

	private String expectedRecordJsonOneRecordPlace1 = getExpectedRecordJsonOneRecordPlace1();

	private String getExpectedRecordJsonOneRecordPlace1() {
		String expectedJson = "{\n";
		expectedJson += "    \"children\": [{\n";
		expectedJson += "        \"children\": [{\n";
		expectedJson += "            \"children\": [\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"name\": \"type\",\n";
		expectedJson += "                    \"value\": \"place\"\n";
		expectedJson += "                },\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"name\": \"id\",\n";
		expectedJson += "                    \"value\": \"place:0001\"\n";
		expectedJson += "                },\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"children\": [\n";
		expectedJson += "                        {\n";
		expectedJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedJson += "                            \"value\": \"system\"\n";
		expectedJson += "                        },\n";
		expectedJson += "                        {\n";
		expectedJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedJson += "                            \"value\": \"cora\"\n";
		expectedJson += "                        }\n";
		expectedJson += "                    ],\n";
		expectedJson += "                    \"name\": \"dataDivider\"\n";
		expectedJson += "                }\n";
		expectedJson += "            ],\n";
		expectedJson += "            \"name\": \"recordInfo\"\n";
		expectedJson += "        }],\n";
		expectedJson += "        \"name\": \"authority\"\n";
		expectedJson += "    }],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		return expectedJson;
	}

	private String expectedRecordJsonOneRecordPlace2 = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
			+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0002\"}"
			+ ",{\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"}"
			+ ",{\"name\":\"linkedRecordId\",\"value\":\"cora\"}],\"name\":\"dataDivider\"}]"
			+ ",\"name\":\"recordInfo\"}" + "],\"name\":\"authority\"}],\"name\":\"recordList\"}";

	private String expectedRecordJsonTwoRecords = getExpectedRecordJsonTwoRecords();
	private RecordStorageOnDisk recordStorage;

	private String getExpectedRecordJsonTwoRecords() {
		String expectedJson = "{\n";
		expectedJson += "    \"children\": [\n";
		expectedJson += "        {\n";
		expectedJson += "            \"children\": [{\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0001\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            }],\n";
		expectedJson += "            \"name\": \"authority\"\n";
		expectedJson += "        },\n";
		expectedJson += "        {\n";
		expectedJson += "            \"children\": [{\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0002\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            }],\n";
		expectedJson += "            \"name\": \"authority\"\n";
		expectedJson += "        }\n";
		expectedJson += "    ],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		return expectedJson;
	}

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles();
		recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}

	private void createRecordTypePlace() {
		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", placeRecordType, emptyCollectedData, emptyLinkList,
				"cora");
	}

	private void deleteFiles() throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(basePath));
		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles();
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void testInitNoFilesOnDisk() throws IOException {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		createRecordTypePlace();

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);
		Path path = Paths.get(basePath, LINK_LISTS_FILENAME);
		assertFalse(Files.exists(path));
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"authority", "place", "place:0001");
	}

	@Test
	public void testInitNoFilesOnDiskTwoSystems() throws IOException {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);

		recordStorage.create("place", "place:0002", dataGroup, emptyCollectedData, emptyLinkList,
				"jsClient");
		assertEquals(readJsonFileFromDisk("place_jsClient.json"), expectedRecordJsonOneRecordPlace1);
	}

	private String readJsonFileFromDisk(String fileName) throws IOException {
		Path path = Paths.get(basePath, fileName);
		BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
		String line = null;
		String json = "";
		while ((line = reader.readLine()) != null) {
			json += line + "\n";
		}
		reader.close();
		return json;
	}

	@Test
	public void testInitNoFilesOnDiskTwoSystemsMoveRecordBetweenSystems() throws IOException {
		DataGroup emptyLinkList = DataGroup.withNameInData("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);

		recordStorage.create("place", "place:0002", dataGroup, emptyCollectedData, emptyLinkList,
				"jsClient");
		assertEquals(readJsonFileFromDisk("place_jsClient.json"), expectedRecordJsonOneRecordPlace1);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		recordStorage.update("place", "place:0002", dataGroup2, emptyLinkList, "cora");
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonTwoRecords);

		Path path = Paths.get(basePath, "place_jsClient.json");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testInitTwoFilesOnDiskTwoSystems() throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, PLACE_FILENAME);
		writeFileToDisk(expectedRecordJsonOneRecordPlace2, "place_jsClient.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		DataGroup dataGroupOut2 = recordStorage.read("place", "place:0002");
		assertJsonEqualDataGroup(dataGroupOut2, dataGroup2);
	}

	@Test
	public void testInitTwoFilesOnDiskTwoSystemsUnrelatedDirectory() throws IOException {
		createRecordTypePlace();
		writeDirectoryToDisk("someUnrelatedDir");
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, PLACE_FILENAME);
		writeFileToDisk(expectedRecordJsonOneRecordPlace2, "place_jsClient.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		DataGroup dataGroupOut2 = recordStorage.read("place", "place:0002");
		assertJsonEqualDataGroup(dataGroupOut2, dataGroup2);
	}

	@Test
	public void testRecordWithLinks() throws IOException {
		createRecordTypePlace();
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);

		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [{\n";
		expectedLinkListJson += "        \"children\": [{\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    },\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    }\n";
		expectedLinkListJson += "                ],\n";
		expectedLinkListJson += "                \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place:0001\"\n";
		expectedLinkListJson += "        }],\n";
		expectedLinkListJson += "        \"name\": \"place\"\n";
		expectedLinkListJson += "    }],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		Path path = Paths.get(basePath, LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME), expectedLinkListJson);
	}

	@Test
	public void testRecordWithLinksOneRecordTypeWithoutLinks() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"cora");
		recordStorage.create("organisation", "organisation:0001", dataGroup, emptyCollectedData,
				emptyLinkList, "cora");

		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [{\n";
		expectedLinkListJson += "        \"children\": [{\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    },\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    }\n";
		expectedLinkListJson += "                ],\n";
		expectedLinkListJson += "                \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place:0001\"\n";
		expectedLinkListJson += "        }],\n";
		expectedLinkListJson += "        \"name\": \"place\"\n";
		expectedLinkListJson += "    }],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		Path path = Paths.get(basePath, LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME), expectedLinkListJson);

	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	@Test
	public void testRecordWithLinksTwoSystems() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"cora");
		recordStorage.create("place", "place:0002", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"jsClient");
		recordStorage.create("place", "place:0003", dataGroup, emptyCollectedData, linkListWithTwoLinks,
				"jsClient");
		recordStorage.create("organisation", "org:0001", dataGroup, emptyCollectedData,
				linkListWithTwoLinks, "cora");
		recordStorage.create("organisation", "org:0002", dataGroup, emptyCollectedData,
				linkListWithTwoLinks, "jsClient");

		Path path = Paths.get(basePath, LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [{\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "                }],\n";
		expectedLinkListJson += "                \"name\": \"org:0001\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"organisation\"\n";
		expectedLinkListJson += "        },\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [{\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "                }],\n";
		expectedLinkListJson += "                \"name\": \"place:0001\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place\"\n";
		expectedLinkListJson += "        }\n";
		expectedLinkListJson += "    ],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME), expectedLinkListJson);

		Path path2 = Paths.get(basePath, "linkLists_jsClient.json");
		assertTrue(Files.exists(path2));
		String expectedLinkListJson2 = "{\n";
		expectedLinkListJson2 += "    \"children\": [\n";
		expectedLinkListJson2 += "        {\n";
		expectedLinkListJson2 += "            \"children\": [{\n";
		expectedLinkListJson2 += "                \"children\": [{\n";
		expectedLinkListJson2 += "                    \"children\": [\n";
		expectedLinkListJson2 += "                        {\n";
		expectedLinkListJson2 += "                            \"children\": [\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                },\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                }\n";
		expectedLinkListJson2 += "                            ],\n";
		expectedLinkListJson2 += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                        },\n";
		expectedLinkListJson2 += "                        {\n";
		expectedLinkListJson2 += "                            \"children\": [\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                },\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                }\n";
		expectedLinkListJson2 += "                            ],\n";
		expectedLinkListJson2 += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                        }\n";
		expectedLinkListJson2 += "                    ],\n";
		expectedLinkListJson2 += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                }],\n";
		expectedLinkListJson2 += "                \"name\": \"org:0002\"\n";
		expectedLinkListJson2 += "            }],\n";
		expectedLinkListJson2 += "            \"name\": \"organisation\"\n";
		expectedLinkListJson2 += "        },\n";
		expectedLinkListJson2 += "        {\n";
		expectedLinkListJson2 += "            \"children\": [\n";
		expectedLinkListJson2 += "                {\n";
		expectedLinkListJson2 += "                    \"children\": [{\n";
		expectedLinkListJson2 += "                        \"children\": [\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            },\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            }\n";
		expectedLinkListJson2 += "                        ],\n";
		expectedLinkListJson2 += "                        \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                    }],\n";
		expectedLinkListJson2 += "                    \"name\": \"place:0002\"\n";
		expectedLinkListJson2 += "                },\n";
		expectedLinkListJson2 += "                {\n";
		expectedLinkListJson2 += "                    \"children\": [{\n";
		expectedLinkListJson2 += "                        \"children\": [\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            },\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            }\n";
		expectedLinkListJson2 += "                        ],\n";
		expectedLinkListJson2 += "                        \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                    }],\n";
		expectedLinkListJson2 += "                    \"name\": \"place:0003\"\n";
		expectedLinkListJson2 += "                }\n";
		expectedLinkListJson2 += "            ],\n";
		expectedLinkListJson2 += "            \"name\": \"place\"\n";
		expectedLinkListJson2 += "        }\n";
		expectedLinkListJson2 += "    ],\n";
		expectedLinkListJson2 += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson2 += "}\n";
		assertEquals(readJsonFileFromDisk("linkLists_jsClient.json"), expectedLinkListJson2);

		recordStorage.update("place", "place:0001", dataGroup, emptyLinkList, "cora");
		recordStorage.update("place", "place:0002", dataGroup, emptyLinkList, "jsClient");
		recordStorage.update("place", "place:0003", dataGroup, emptyLinkList, "jsClient");
		recordStorage.update("organisation", "org:0001", dataGroup, emptyLinkList, "cora");
		recordStorage.update("organisation", "org:0002", dataGroup, emptyLinkList, "jsClient");

		assertFalse(Files.exists(path));
		assertFalse(Files.exists(path2));
	}

	@Test
	public void testWriteCollectedTermsToDiskOneRecordOneTerm() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place", "place:0001");
		DataGroup collectStorageTerm = DataGroup.withNameInData("collectStorageTerm");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("place", "place:0001", dataGroup, collectedData, emptyLinkList, "cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [{\n";
		expectedCollectedDataOneTerm += "        \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "        \"children\": [\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "            }\n";
		expectedCollectedDataOneTerm += "        ],\n";
		expectedCollectedDataOneTerm += "        \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "    }],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME), expectedCollectedDataOneTerm);
	}

	@Test
	public void testWriteCollectedTermsToDiskTwoRecordOneTerm() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place", "place:0001");
		DataGroup collectStorageTerm = DataGroup.withNameInData("collectStorageTerm");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("place", "place:0001", dataGroup, collectedData, emptyLinkList, "cora");

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		DataGroup collectedData2 = DataCreator.createCollectedDataWithTypeAndId("place", "place:0002");
		DataGroup collectStorageTerm2 = DataGroup.withNameInData("collectStorageTerm");
		collectedData2.addChild(collectStorageTerm2);

		DataGroup collectedDataTerm2 = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm2.addChild(collectedDataTerm2);

		recordStorage.create("place", "place:0002", dataGroup2, collectedData2, emptyLinkList, "cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        },\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"1\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0002\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        }\n";
		expectedCollectedDataOneTerm += "    ],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME), expectedCollectedDataOneTerm);
	}

	@Test
	public void testInitWithFileOnDiskNoLinksOnDisk() {
		createRecordTypePlace();
		writePlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		assertJsonEqualDataGroup(linkListPlace, emptyLinkList);
	}

	private void assertJsonEqualDataGroup(DataGroup dataGroupActual, DataGroup dataGroupExpected) {
		assertEquals(convertDataGroupToJsonString(dataGroupActual),
				convertDataGroupToJsonString(dataGroupExpected));
	}

	private void writePlaceFileToDisk() {
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, PLACE_FILENAME);
	}

	private void writeFileToDisk(String json, String fileName) {
		Path path = FileSystems.getDefault().getPath(basePath, fileName);
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
			writer.close();
		} catch (IOException e) {
		}
	}

	private void writeDirectoryToDisk(String dirName) {
		Path path = FileSystems.getDefault().getPath(basePath, dirName);
		try {
			Files.createDirectory(path);
		} catch (IOException e) {
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = convertDataGroupToJson(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter convertDataGroupToJson(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	@Test
	public void testInitPlaceAndPersonFileOnDisk() {
		createRecordTypePlace();
		DataGroup personRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("person", "true", "false");
		recordStorage.create("recordType", "person", personRecordType, emptyCollectedData, emptyLinkList,
				"cora");
		writePlaceFileToDisk();
		writePersonFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), "authority");
		DataGroup dataGroupPersonOut = recordStorage.read("person", "person:0001");
		assertEquals(dataGroupPersonOut.getNameInData(), "authority");
	}

	private void writePersonFileToDisk() {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"person\"}" + ",{\"name\":\"id\",\"value\":\"person:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = PERSON_FILENAME;
		writeFileToDisk(json, fileName);
	}

	@Test
	public void testUpdate() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");

		dataGroup.addChild(DataAtomic.withNameInDataAndValue("someNameInData", "someValue"));
		recordStorage.update("place", "place:0001", dataGroup, emptyLinkList, "cora");

		String expectedJson = "{\n";
		expectedJson += "    \"children\": [{\n";
		expectedJson += "        \"children\": [\n";
		expectedJson += "            {\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0001\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            },\n";
		expectedJson += "            {\n";
		expectedJson += "                \"name\": \"someNameInData\",\n";
		expectedJson += "                \"value\": \"someValue\"\n";
		expectedJson += "            }\n";
		expectedJson += "        ],\n";
		expectedJson += "        \"name\": \"authority\"\n";
		expectedJson += "    }],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedJson);
	}

	@Test
	public void testDelete() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		Path path = Paths.get(basePath, PLACE_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeleteRemoveOneRecord() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		recordStorage.create("place", "place:0002", dataGroup2, emptyCollectedData, emptyLinkList,
				"cora");

		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonTwoRecords);

		recordStorage.deleteByTypeAndId("place", "place:0002");
		assertEquals(readJsonFileFromDisk(PLACE_FILENAME), expectedRecordJsonOneRecordPlace1);
	}

	@Test
	public void testDeleteFileOnDiskRemovedWhenNoRecordsLeft() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		Path path = Paths.get(basePath, PLACE_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testInitWithFileOnDiskLinksOnDisk() {
		createRecordTypePlace();
		writePlaceFileToDisk();
		writePlaceLinksFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"from\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                }\n";
		expectedLinkListJson += "            ],\n";
		expectedLinkListJson += "            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "        },\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"from\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                }\n";
		expectedLinkListJson += "            ],\n";
		expectedLinkListJson += "            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "        }\n";
		expectedLinkListJson += "    ],\n";
		expectedLinkListJson += "    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "}";
		assertEquals(convertDataGroupToJsonString(linkListPlace), expectedLinkListJson);

		DataGroup dataGroupTo = createDataGroupWithRecordInfo();
		recordStorage.create("toRecordType", "toRecordId", dataGroupTo, emptyCollectedData,
				emptyLinkList, "cora");
		Collection<DataGroup> incomingLinksTo = recordStorage
				.generateLinkCollectionPointingToRecord("toRecordType", "toRecordId");

		assertEquals(incomingLinksTo.size(), 1);
	}

	private void writePlaceLinksFileToDisk() {
		String expectedLinkListJson = "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]" + ",\"name\":\"to\"}]"
				+ ",\"name\":\"recordToRecordLink\"}" + ",{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
				+ ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		writeFileToDisk(expectedLinkListJson, LINK_LISTS_FILENAME);

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testCreateMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		removeTempFiles();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testUpdateMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		removeTempFiles();
		recordStorage.update("place", "place:0001", dataGroup, emptyLinkList, "cora");

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDeleteMissingPath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, emptyCollectedData, emptyLinkList,
				"cora");
		removeTempFiles();
		recordStorage.deleteByTypeAndId("place", "place:0001");

	}
}
