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

package se.uu.ub.cora.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.storage.testdata.TestDataAppTokenStorage;

public class AppTokenStorageTest {
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private Map<String, String> initInfo;
	private AppTokenStorageImp appTokenStorage;

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles();
		TestDataAppTokenStorage.createRecordStorageInMemoryWithTestData(basePath);

		initInfo = new HashMap<>();
		initInfo.put("storageOnDiskBasePath", basePath);
		appTokenStorage = new AppTokenStorageImp(initInfo);
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
	public void test() {
		assertTrue(appTokenStorage.recordStorage instanceof RecordStorageInMemoryReadFromDisk);
	}

	@Test(expectedExceptions = RuntimeException.class)
	public void testInitNoStorageOnDiskBasePath() {
		Map<String, String> initInfo = new HashMap<>();
		new AppTokenStorageImp(initInfo);
	}

	@Test
	public void testGetAppTokenForUser() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("dummy1");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.get(0), "someSecretString");
	}

	@Test
	public void testGetAppTokenForUser2() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("dummy2");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.get(0), "someOtherSecretString");
	}

	@Test
	public void testGetAppTokenForNonExistingUser() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("nonExistingUser");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.size(), 0);
	}

	@Test
	public void testGetAppTokenForInactiveUser() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("inactiveUser");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.size(), 0);
	}

	@Test
	public void testGetAppTokenForUserNoAppToken() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("noAppTokenUser");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.size(), 0);
	}

	@Test
	public void testGetAppTokenForUserNoAppTokenRepopulatesOnceFromDisk() {
		List<String> apptokensForUser = appTokenStorage.getAppTokensForUserId("noAppTokenUser");
		assertNotNull(apptokensForUser);
		assertEquals(apptokensForUser.size(), 0);
		assertEquals(appTokenStorage.getNoOfReadsFromDisk(), 2);
	}

}
