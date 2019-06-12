/*
 * Copyright 2015 Uppsala University Library
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

package se.uu.ub.cora.basicstorage.testdata;

import se.uu.ub.cora.data.DataAtomic;
import se.uu.ub.cora.data.DataGroup;

public final class DataCreator {
	private static final String SELF_PRESENTATION_VIEW_ID = "selfPresentationViewId";
	private static final String USER_SUPPLIED_ID = "userSuppliedId";
	private static final String SEARCH_PRESENTATION_FORM_ID = "searchPresentationFormId";
	private static final String SEARCH_METADATA_ID = "searchMetadataId";
	private static final String LIST_PRESENTATION_VIEW_ID = "listPresentationViewId";
	private static final String NEW_PRESENTATION_FORM_ID = "newPresentationFormId";
	private static final String PRESENTATION_FORM_ID = "presentationFormId";
	private static final String PRESENTATION_VIEW_ID = "presentationViewId";
	private static final String METADATA_ID = "metadataId";
	private static final String NEW_METADATA_ID = "newMetadataId";
	private static final String RECORD_TYPE = "recordType";

	public static DataGroup createRecordTypeWithIdAndUserSuppliedIdAndAbstract(String id,
			String userSuppliedId, String abstractValue) {
		return createRecordTypeWithIdAndUserSuppliedIdAndAbstractAndParentId(id, userSuppliedId,
				abstractValue, null);
	}

	private static DataGroup createRecordTypeWithIdAndUserSuppliedIdAndAbstractAndParentId(
			String id, String userSuppliedId, String abstractValue, String parentId) {
		String idWithCapitalFirst = id.substring(0, 1).toUpperCase() + id.substring(1);

		DataGroup dataGroup = DataGroup.withNameInData(RECORD_TYPE);
		dataGroup.addChild(createRecordInfoWithRecordTypeAndRecordId(RECORD_TYPE, id));

		dataGroup.addChild(
				createChildWithNamInDataLinkedTypeLinkedId(METADATA_ID, "metadataGroup", id));

		dataGroup.addChild(createChildWithNamInDataLinkedTypeLinkedId(PRESENTATION_VIEW_ID,
				"presentationGroup", "pg" + idWithCapitalFirst + "View"));

		dataGroup.addChild(createChildWithNamInDataLinkedTypeLinkedId(PRESENTATION_FORM_ID,
				"presentationGroup", "pg" + idWithCapitalFirst + "Form"));
		dataGroup.addChild(createChildWithNamInDataLinkedTypeLinkedId(NEW_METADATA_ID,
				"metadataGroup", id + "New"));

		dataGroup.addChild(createChildWithNamInDataLinkedTypeLinkedId(NEW_PRESENTATION_FORM_ID,
				"presentationGroup", "pg" + idWithCapitalFirst + "FormNew"));
		dataGroup.addChild(createChildWithNamInDataLinkedTypeLinkedId(LIST_PRESENTATION_VIEW_ID,
				"presentationGroup", "pg" + idWithCapitalFirst + "List"));
		dataGroup.addChild(DataAtomic.withNameInDataAndValue(SEARCH_METADATA_ID, id + "Search"));
		dataGroup.addChild(DataAtomic.withNameInDataAndValue(SEARCH_PRESENTATION_FORM_ID,
				"pg" + idWithCapitalFirst + "SearchForm"));

		dataGroup.addChild(DataAtomic.withNameInDataAndValue(USER_SUPPLIED_ID, userSuppliedId));
		dataGroup.addChild(DataAtomic.withNameInDataAndValue(SELF_PRESENTATION_VIEW_ID,
				"pg" + idWithCapitalFirst + "Self"));
		dataGroup.addChild(DataAtomic.withNameInDataAndValue("abstract", abstractValue));
		if (null != parentId) {
			dataGroup.addChild(
					createChildWithNamInDataLinkedTypeLinkedId("parentId", "recordType", parentId));
		}
		return dataGroup;
	}

	private static DataGroup createChildWithNamInDataLinkedTypeLinkedId(String nameInData,
			String linkedRecordType, String id) {
		DataGroup metadataId = DataGroup.withNameInData(nameInData);
		metadataId
				.addChild(DataAtomic.withNameInDataAndValue("linkedRecordType", linkedRecordType));
		metadataId.addChild(DataAtomic.withNameInDataAndValue("linkedRecordId", id));
		return metadataId;
	}

	public static DataGroup createRecordTypeWithIdAndUserSuppliedIdAndParentId(String id,
			String userSuppliedId, String parentId) {
		return createRecordTypeWithIdAndUserSuppliedIdAndAbstractAndParentId(id, userSuppliedId,
				"false", parentId);
	}

	public static DataGroup createRecordInfoWithRecordTypeAndRecordId(String recordType,
			String recordId) {
		DataGroup recordInfo = DataGroup.withNameInData("recordInfo");
		recordInfo.addChild(DataAtomic.withNameInDataAndValue("type", recordType));
		recordInfo.addChild(DataAtomic.withNameInDataAndValue("id", recordId));

		DataGroup dataDivider = DataGroup.withNameInData("dataDivider");
		dataDivider.addChild(DataAtomic.withNameInDataAndValue("linkedRecordType", "system"));
		dataDivider.addChild(DataAtomic.withNameInDataAndValue("linkedRecordId", "cora"));
		recordInfo.addChild(dataDivider);
		return recordInfo;
	}

	public static DataGroup createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
			String nameInData, String recordType, String recordId) {
		DataGroup dataGroup = DataGroup.withNameInData(nameInData);
		dataGroup.addChild(createRecordInfoWithRecordTypeAndRecordId(recordType, recordId));
		return dataGroup;
	}

	public static DataGroup createEmptyLinkList() {
		return DataGroup.withNameInData("collectedDataLinks");
	}

	public static DataGroup createRecordToRecordLink(String fromRecordType, String fromRecordId,
			String toRecordType, String toRecordId) {
		DataGroup recordToRecordLink = DataGroup.withNameInData("recordToRecordLink");

		DataGroup from = DataGroup.withNameInData("from");
		recordToRecordLink.addChild(from);

		DataAtomic fromLinkedRecordType = DataAtomic.withNameInDataAndValue("linkedRecordType",
				fromRecordType);
		from.addChild(fromLinkedRecordType);

		DataAtomic fromLinkedRecordId = DataAtomic.withNameInDataAndValue("linkedRecordId",
				fromRecordId);
		from.addChild(fromLinkedRecordId);

		DataGroup to = DataGroup.withNameInData("to");
		recordToRecordLink.addChild(to);

		DataAtomic toLinkedRecordType = DataAtomic.withNameInDataAndValue("linkedRecordType",
				toRecordType);
		to.addChild(toLinkedRecordType);

		DataAtomic toLinkedRecordId = DataAtomic.withNameInDataAndValue("linkedRecordId",
				toRecordId);
		to.addChild(toLinkedRecordId);

		return recordToRecordLink;
	}

	public static DataGroup createEmptyCollectedData() {
		return DataGroup.withNameInData("collectedData");
	}

	public static DataGroup createCollectedDataWithTypeAndId(String type, String id) {
		DataGroup collectedData = DataGroup.withNameInData("collectedData");
		collectedData.addChild(DataAtomic.withNameInDataAndValue("type", type));
		collectedData.addChild(DataAtomic.withNameInDataAndValue("id", id));
		return collectedData;
	}

	public static DataGroup createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey(
			String repeatId, String termId, String termValue, String storageKey) {
		DataGroup collectedDataTerm = DataGroup.withNameInData("collectedDataTerm");
		collectedDataTerm.addAttributeByIdWithValue("type", "storage");
		collectedDataTerm.setRepeatId(repeatId);

		DataAtomic collectedDataTermId = DataAtomic.withNameInDataAndValue("collectTermId", termId);
		collectedDataTerm.addChild(collectedDataTermId);

		DataAtomic collectedDataTermValue = DataAtomic.withNameInDataAndValue("collectTermValue",
				termValue);
		collectedDataTerm.addChild(collectedDataTermValue);
		DataGroup extraData = DataGroup.withNameInData("extraData");
		collectedDataTerm.addChild(extraData);
		extraData.addChild(DataAtomic.withNameInDataAndValue("storageKey", storageKey));
		return collectedDataTerm;
	}

	public static DataGroup createEmptyFilter() {
		return DataGroup.withNameInData("filter");
	}

	public static DataGroup createFilterPartWithRepeatIdAndKeyAndValue(String repeatId, String key,
			String value) {
		DataGroup part = DataGroup.withNameInData("part");
		part.setRepeatId(repeatId);
		part.addChild(DataAtomic.withNameInDataAndValue("key", key));
		part.addChild(DataAtomic.withNameInDataAndValue("value", value));
		return part;
	}
}
