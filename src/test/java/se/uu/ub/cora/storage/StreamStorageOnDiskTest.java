/*
 * Copyright 2016 Uppsala University Library
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

import java.io.ByteArrayInputStream;
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

import se.uu.ub.cora.spider.stream.storage.StreamStorage;

public class StreamStorageOnDiskTest {
	private String basePath = "/tmp/streamStorageOnDiskTemp/";

	@BeforeMethod
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

	@Test
	public void testUploadCreatePathForNewDataDivider() {
		StreamStorage streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		streamStorage.store("someStreamId", "someDataDivider", stream);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider")));
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testInitNoPermissionOnPath() throws IOException {
		removeTempFiles();
		StreamStorage streamStorage = StreamStorageOnDisk.usingBasePath("/root");
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		streamStorage.store("someStreamId", "someDataDivider", stream);
	}

	// @Test(expectedExceptions = DataStorageException.class)
	@Test
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		StreamStorage streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		streamStorage.store("someStreamId", "someDataDivider", stream);
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testUploadCreateFileForStreamPathIsEmpty() throws IOException {
		// removeTempFiles();
		StreamStorageOnDisk streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		streamStorage.tryToStoreStream(stream, Paths.get(""));
	}

	@Test
	public void testUploadCreateFileForStream() {
		StreamStorage streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		long size = streamStorage.store("someStreamId", "someDataDivider", stream);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId")));
		assertEquals(String.valueOf(size), "8");
	}

	@Test
	public void testUploadCreateFileForStreamDirectoriesAlreadyExist() {
		StreamStorage streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		long size = streamStorage.store("someStreamId", "someDataDivider", stream);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId")));
		assertEquals(String.valueOf(size), "8");

		InputStream stream2 = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		long size2 = streamStorage.store("someStreamId2", "someDataDivider", stream2);
		assertTrue(Files.exists(Paths.get(basePath, "someDataDivider", "someStreamId2")));
		assertEquals(String.valueOf(size2), "8");
	}

}
