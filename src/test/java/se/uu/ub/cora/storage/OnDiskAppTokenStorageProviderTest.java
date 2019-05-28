/*
 * Copyright 2019 Olov McKie
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

import se.uu.ub.cora.apptokenstorage.AppTokenStorage;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.storage.log.LoggerFactorySpy;
import se.uu.ub.cora.storage.testdata.TestDataAppTokenStorage;

public class OnDiskAppTokenStorageProviderTest {

	private OnDiskAppTokenStorageProvider onDiskAppTokenStorageProvider;
	private Map<String, String> initInfo = new HashMap<>();
	private String basePath = "/tmp/recordStorageOnDiskTempApptokenStorageProvider/";
	private LoggerFactorySpy loggerFactorySpy;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);
		initInfo = new HashMap<>();
		initInfo.put("storageOnDiskBasePath", basePath);
		makeSureBasePathExistsAndIsEmpty();
		onDiskAppTokenStorageProvider = new OnDiskAppTokenStorageProvider();
		onDiskAppTokenStorageProvider.startUsingInitInfo(initInfo);
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);
		TestDataAppTokenStorage.createRecordStorageInMemoryWithTestData(basePath);

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
	public void testGetOrderToSelectImplementationsBy() throws Exception {
		assertEquals(onDiskAppTokenStorageProvider.getOrderToSelectImplementionsBy(), 0);
	}

	@Test
	public void testGetAppTokenStorage() throws Exception {
		AppTokenStorage appTokenStorage = onDiskAppTokenStorageProvider.getAppTokenStorage();
		assertTrue(appTokenStorage instanceof AppTokenStorageImp);
	}
}
