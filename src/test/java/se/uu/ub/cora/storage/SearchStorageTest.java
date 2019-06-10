/*
 * Copyright 2017, 2019 Uppsala University Library
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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.searchstorage.SearchStorage;
import se.uu.ub.cora.storage.testdata.TestDataRecordInMemoryStorage;

public class SearchStorageTest {
	private SearchStorage searchStorage;

	@BeforeMethod
	public void BeforeMethod() {

		RecordStorageInMemory recordStorageInMemory = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		searchStorage = recordStorageInMemory;
	}

	@Test
	public void testGetSearchTerm() {
		DataGroup userGroup = searchStorage.getSearchTerm("someSearchTerm");
		assertEquals(userGroup.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "someSearchTerm");
	}

	@Test
	public void testGetCollectIndexTerm() {
		DataGroup collectIndexTerm = searchStorage.getCollectIndexTerm("collectIndexTermId");
		DataGroup recordInfo = collectIndexTerm.getFirstGroupWithNameInData("recordInfo");
		assertEquals(recordInfo.getFirstAtomicValueWithNameInData("id"), "collectIndexTermId");
		assertEquals(recordInfo.getFirstAtomicValueWithNameInData("type"), "collectIndexTerm");

	}
}
