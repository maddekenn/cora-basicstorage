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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import se.uu.ub.cora.spider.stream.storage.StreamStorage;

public final class StreamStorageOnDisk implements StreamStorage {

	private static final int BUFFER_LENGTH = 1024;
	private String basePath;

	public static StreamStorageOnDisk usingBasePath(String basePath) {
		return new StreamStorageOnDisk(basePath);
	}

	private StreamStorageOnDisk(String basePath) {
		this.basePath = basePath;
	}

	@Override
	public long store(String streamId, String dataDivider, InputStream stream) {
		Path pathByDataDivider = Paths.get(basePath, dataDivider);
		ensureStorageDirectoryExists(pathByDataDivider);

		Path path = Paths.get(basePath, dataDivider, streamId);
		return tryToStoreStream(stream, path);
	}

	long tryToStoreStream(InputStream stream, Path path) {
		try {
			return storeStream(stream, path);
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not write files to disk" + e);
		}
	}

	private long storeStream(InputStream stream, Path path) throws IOException {
		OutputStream outputStream = Files.newOutputStream(path);
		long size = storeStreamUsingOutputStream(stream, outputStream);
		outputStream.flush();
		outputStream.close();
		return size;
	}

	private long storeStreamUsingOutputStream(InputStream stream, OutputStream outputStream)
			throws IOException {
		long size = 0;
		byte[] bytes = new byte[BUFFER_LENGTH];

		int written = 0;
		while ((written = stream.read(bytes)) != -1) {
			outputStream.write(bytes, 0, written);
			size += written;
		}
		return size;
	}

	private void ensureStorageDirectoryExists(Path pathByDataDivider) {
		if (storageDirectoryDoesNotExist(pathByDataDivider)) {
			tryToCreateStorageDirectory(pathByDataDivider);
		}
	}

	private boolean storageDirectoryDoesNotExist(Path pathByDataDivider) {
		return !Files.exists(pathByDataDivider);
	}

	private void tryToCreateStorageDirectory(Path pathByDataDivider) {
		try {
			Files.createDirectory(pathByDataDivider);
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not write files to disk" + e);
		}
	}

}
