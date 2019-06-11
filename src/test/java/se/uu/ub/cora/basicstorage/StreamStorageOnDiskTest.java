/*
 * Copyright 2016 Uppsala University Library
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

package se.uu.ub.cora.basicstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.storage.StreamStorage;

public class StreamStorageOnDiskTest {
	private String basePath = "/tmp/streamStorageOnDiskTempStream/";
	private StreamStorage streamStorage;
	private InputStream streamToStore;

	@BeforeMethod
	public void setUpForTests() throws IOException {
		makeSureBasePathExistsAndIsEmpty();
		streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		streamToStore = createTestInputStreamToStore();
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);
	}

	private void deleteFiles(String path) throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(path));
		list.forEach(p -> {
			try {
				deleteFile(p);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		list.close();
	}

	private void deleteFile(Path path) throws IOException {
		if (new File(path.toString()).isDirectory()) {
			deleteFiles(path.toString());
		}
		try {
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles(basePath);
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testInitNoPermissionOnPath() throws IOException {
		removeTempFiles();
		StreamStorageOnDisk.usingBasePath("/root/streamsDOESNOTEXIST");
	}

	@Test
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		StreamStorageOnDisk.usingBasePath(basePath);
	}

	@Test
	public void testGetBasePath() throws IOException {
		StreamStorageOnDisk streamStorageOnDisk = (StreamStorageOnDisk) streamStorage;
		assertEquals(streamStorageOnDisk.getBasePath(), basePath);
	}

	@Test
	public void testUploadCreatePathForNewDataDivider() {
		streamStorage.store("someStreamId", "someDataDivider", streamToStore);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider")));
	}

	private InputStream createTestInputStreamToStore() {
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		return stream;
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testUploadCreateFileForStreamPathIsEmpty() throws IOException {
		((StreamStorageOnDisk) streamStorage).tryToStoreStream(streamToStore, Paths.get(""));
	}

	@Test
	public void testUploadCreateFileForStream() {
		long size = streamStorage.store("someStreamId", "someDataDivider", streamToStore);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId")));
		assertEquals(String.valueOf(size), "8");
	}

	@Test
	public void testUploadCreateFileForStreamDirectoriesAlreadyExist() {
		long size = streamStorage.store("someStreamId", "someDataDivider", streamToStore);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId")));
		assertEquals(String.valueOf(size), "8");

		InputStream stream2 = createTestInputStreamToStore();
		long size2 = streamStorage.store("someStreamId2", "someDataDivider", stream2);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId2")));
		assertEquals(String.valueOf(size2), "8");
	}

	@Test
	public void testDownload() throws IOException {
		streamStorage.store("someStreamId", "someDataDivider", streamToStore);

		InputStream stream = streamStorage.retrieve("someStreamId", "someDataDivider");
		assertNotNull(stream);

		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = stream.read(buffer)) != -1) {
			result.write(buffer, 0, length);
		}
		String stringFromStream = result.toString("UTF-8");

		assertEquals(stringFromStream, "a string");
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDownloadFolderForDataDividerIsMissing() {
		streamStorage.retrieve("someStreamId", "someDataDivider");
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDownloadStreamIsMissing() {
		streamStorage.store("someStreamId", "someDataDivider", streamToStore);
		streamStorage.retrieve("someStreamIdDOESNOTEXIST", "someDataDivider");
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDownloadPathForStreamIsBroken() throws IOException {
		((StreamStorageOnDisk) streamStorage).tryToReadStream(Paths.get("/broken/path"));
	}
}
