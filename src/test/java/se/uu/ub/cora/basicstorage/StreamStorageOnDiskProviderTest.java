/*
 * Copyright 2019 Uppsala University Library
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
package se.uu.ub.cora.basicstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.log.LoggerFactorySpy;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.storage.StreamStorage;
import se.uu.ub.cora.storage.StreamStorageProvider;

public class StreamStorageOnDiskProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private String basePath = "/tmp/streamOnDiskTempBasicStorageProvider/";
	private LoggerFactorySpy loggerFactorySpy;
	private String testedClassName = "StreamStorageOnDiskProvider";
	private StreamStorageProvider streamStorageOnDiskProvider;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);
		initInfo = new HashMap<>();
		initInfo.put("storageOnDiskBasePath", basePath);
		makeSureBasePathExistsAndIsEmpty();
		streamStorageOnDiskProvider = new StreamStorageOnDiskProvider();
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);

	}

	private void deleteFiles(String path) throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(path));

		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			if (path.toFile().isDirectory()) {
				deleteFiles(path.toString());
			}
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetOrderToSelectImplementationsByIsZero() {
		assertEquals(streamStorageOnDiskProvider.getOrderToSelectImplementionsBy(), 0);
	}

	@Test
	public void testNormalStartupReturnsStreamStorageOnDisk() {
		streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
		StreamStorage streamStorage = streamStorageOnDiskProvider.getStreamStorage();
		assertTrue(streamStorage instanceof StreamStorageOnDisk);
	}

	@Test
	public void testNormalStartupBasePathSentToStreamStorage() {
		streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
		StreamStorageOnDisk streamStorage = (StreamStorageOnDisk) streamStorageOnDiskProvider
				.getStreamStorage();
		assertEquals(streamStorage.getBasePath(),
				initInfo.get("storageOnDiskBasePath") + "streams/");
	}

	@Test
	public void testNormalStartupReturnsTheSameStreamStorageForMultipleCalls() {
		streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
		StreamStorage streamStorage = streamStorageOnDiskProvider.getStreamStorage();
		StreamStorage streamStorage2 = streamStorageOnDiskProvider.getStreamStorage();
		assertSame(streamStorage, streamStorage2);
	}

	@Test
	public void testLoggingNormalStartup() {
		streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"StreamStorageOnDiskProvider starting StreamStorageOnDisk...");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 1),
				"Found /tmp/streamOnDiskTempBasicStorageProvider/ as storageOnDiskBasePath");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 2),
				"StreamStorageOnDiskProvider started StreamStorageOnDisk");
		assertEquals(loggerFactorySpy.getNoOfInfoLogMessagesUsingClassName(testedClassName), 3);
	}

	@Test
	public void testLoggingAndErrorIfMissingStartParameters() {
		initInfo.remove("storageOnDiskBasePath");
		try {
			streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
		} catch (Exception e) {

		}
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"StreamStorageOnDiskProvider starting StreamStorageOnDisk...");
		assertEquals(loggerFactorySpy.getNoOfInfoLogMessagesUsingClassName(testedClassName), 1);
		assertEquals(loggerFactorySpy.getFatalLogMessageUsingClassNameAndNo(testedClassName, 0),
				"InitInfo must contain storageOnDiskBasePath");
		assertEquals(loggerFactorySpy.getNoOfFatalLogMessagesUsingClassName(testedClassName), 1);
	}

	@Test(expectedExceptions = DataStorageException.class, expectedExceptionsMessageRegExp = ""
			+ "InitInfo must contain storageOnDiskBasePath")
	public void testErrorIfMissingStartParameters() {
		initInfo.remove("storageOnDiskBasePath");
		streamStorageOnDiskProvider.startUsingInitInfo(initInfo);
	}

}
