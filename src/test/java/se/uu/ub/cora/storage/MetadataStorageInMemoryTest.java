/*
 * Copyright 2015, 2019 Uppsala University Library
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

import java.util.Collection;
import java.util.Iterator;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.bookkeeper.storage.MetadataStorage;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.storage.testdata.DataCreator;
import se.uu.ub.cora.storage.testdata.TestDataRecordInMemoryStorage;

public class MetadataStorageInMemoryTest {

	private MetadataStorage metadataStorage;
	private RecordStorageInMemory recordStorageInMemory;
	DataGroup emptyCollectedData = DataCreator.createEmptyCollectedData();

	@BeforeMethod
	public void BeforeMethod() {
		recordStorageInMemory = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		metadataStorage = recordStorageInMemory;
	}

	@Test
	public void testGetMetadataElements() {
		Collection<DataGroup> metadataElements = metadataStorage.getMetadataElements();
		DataGroup metadataElement = metadataElements.iterator().next();
		assertEquals(metadataElement.getNameInData(), "metadata");
		assertIdInRecordInfoIsCorrect(metadataElement, "place");
	}

	private void assertIdInRecordInfoIsCorrect(DataGroup text, String expectedId) {
		DataGroup recordInfo = text.getFirstGroupWithNameInData("recordInfo");
		String id = recordInfo.getFirstAtomicValueWithNameInData("id");
		assertEquals(id, expectedId);
	}

	@Test
	public void testGetPresentationElements() {
		Collection<DataGroup> presentationElements = metadataStorage.getPresentationElements();
		DataGroup presentationElement = presentationElements.iterator().next();
		assertEquals(presentationElement.getNameInData(), "presentation");
		assertIdInRecordInfoIsCorrect(presentationElement, "placeView");
	}

	@Test
	public void testGetTexts() {
		Collection<DataGroup> texts = metadataStorage.getTexts();
		DataGroup text = texts.iterator().next();
		assertEquals(text.getNameInData(), "text");
		assertIdInRecordInfoIsCorrect(text, "placeText");
	}

	@Test
	public void testGetRecordTypes() {
		Collection<DataGroup> recordTypes = metadataStorage.getRecordTypes();
		Iterator<DataGroup> iterator = recordTypes.iterator();
		DataGroup recordType = iterator.next();

		assertEquals(recordType.getNameInData(), "recordType");
		assertIdInRecordInfoIsCorrect(recordType, "collectTerm");

		recordType = iterator.next();
		assertEquals(recordType.getNameInData(), "recordType");
		assertIdInRecordInfoIsCorrect(recordType, "image");
	}

	// @Test(expectedExceptions = RecordNotFoundException.class)
	// public void testGetCollectTermsWhitoutCollectTerms() {
	// metadataStorage.getCollectTerms();
	// }
	//
	// @Test
	// public void testGetCollectTerms() {
	// Exception e1 = emptyCollectedData;
	// try {
	// metadataStorage.getCollectTerms();
	// } catch (Exception e) {
	// e1 = e;
	// }
	// StackTraceElement[] stackTrace = e1.getStackTrace();
	// assertEquals(stackTrace[1].getMethodName(), "readAbstractList");
	// }
	@Test
	public void testGetCollectTermsWithoutCollectTerms() {
		Collection<DataGroup> collectTerms = metadataStorage.getCollectTerms();
		assertEquals(collectTerms.size(), 1);
	}

	@Test
	public void testGetCollectTermsWithCollectIndexTerm() {
		DataGroup collectIndexTerm = DataCreator
				.createRecordInfoWithRecordTypeAndRecordId("collectIndexTerm", "someIndexTerm");
		recordStorageInMemory.create("collectIndexTerm", "someIndexTerm", collectIndexTerm,
				emptyCollectedData, DataGroup.withNameInData("collectedLinksList"), "cora");

		Collection<DataGroup> collectTerms = metadataStorage.getCollectTerms();
		assertEquals(collectTerms.size(), 2);
	}

	@Test
	public void testGetCollectTermsWithCollectPermissionTerm() {
		DataGroup collectPermissionTerm = DataCreator.createRecordInfoWithRecordTypeAndRecordId(
				"collectPermissionTerm", "somePermissionTerm");
		recordStorageInMemory.create("collectPermissionTerm", "somePermissionTerm",
				collectPermissionTerm, emptyCollectedData,
				DataGroup.withNameInData("collectedLinksList"), "cora");

		Collection<DataGroup> collectTerms = metadataStorage.getCollectTerms();
		assertEquals(collectTerms.size(), 2);
	}

}
