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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import se.uu.ub.cora.apptokenstorage.AppTokenStorage;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;

public class AppTokenStorageImp implements AppTokenStorage {

	protected static final String RECORD_TYPE = "recordType";
	private static final String PARENT_ID = "parentId";
	RecordStorageInMemoryReadFromDisk recordStorage;
	private List<String> userRecordTypeNames = new ArrayList<>();
	private final String basePath;
	private int noOfReadsFromDisk = 0;

	public AppTokenStorageImp(Map<String, String> initInfo) {
		if (!initInfo.containsKey("storageOnDiskBasePath")) {
			throw new RuntimeException("initInfo must contain storageOnDiskBasePath");
		}
		basePath = initInfo.get("storageOnDiskBasePath");
		populateFromStorage();
	}

	private void populateFromStorage() {
		noOfReadsFromDisk++;
		recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		populateUserRecordTypeNameList();
	}

	@Override
	public boolean userIdHasAppToken(String userId, String appToken) {
		if (populatedDataUserIdHasAppToken(userId, appToken)) {
			return true;
		}
		populateFromStorage();
		return populatedDataUserIdHasAppToken(userId, appToken);
	}

	private boolean populatedDataUserIdHasAppToken(String userId, String appToken) {
		List<String> appTokensForUser = findUserAndGetAppTokens(userId);
		return apptokenFoundInList(appToken, appTokensForUser);
	}

	private boolean apptokenFoundInList(String appToken, List<String> tokenList) {
		return tokenList.stream().anyMatch(token -> token.equals(appToken));
	}

	private List<String> findUserAndGetAppTokens(String userId) {
		DataGroup user = findUser(userId);
		return getAppTokensForUser(user);
	}

	private DataGroup findUser(String userId) {
		DataGroup user = null;
		for (String userRecordTypeName : userRecordTypeNames) {
			try {
				user = recordStorage.read(userRecordTypeName, userId);
			} catch (RecordNotFoundException e) {
				// do nothing
			}
		}
		return user;
	}

	private List<String> getAppTokensForUser(DataGroup user) {
		if (userExistsAndIsActive(user)) {
			return getAppTokensForActiveUser(user);
		}
		return new ArrayList<>();
	}

	private boolean userExistsAndIsActive(DataGroup user) {
		return user != null && userIsActive(user);
	}

	private List<String> getAppTokensForActiveUser(DataGroup user) {
		List<DataGroup> userAppTokenGroups = user.getAllGroupsWithNameInData("userAppTokenGroup");
		return getAppTokensForAppTokenGroups(userAppTokenGroups);
	}

	private List<String> getAppTokensForAppTokenGroups(List<DataGroup> userAppTokenGroups) {
		List<String> apptokens = new ArrayList<>();
		for (DataGroup userAppTokenGroup : userAppTokenGroups) {
			String appTokenId = extractAppTokenId(userAppTokenGroup);
			apptokens.add(getTokenFromStorage(appTokenId));
		}
		return apptokens;
	}

	private String extractAppTokenId(DataGroup userAppTokenGroup) {
		return userAppTokenGroup.getFirstGroupWithNameInData("appTokenLink")
				.getFirstAtomicValueWithNameInData("linkedRecordId");
	}

	private String getTokenFromStorage(String appTokenId) {
		return recordStorage.read("appToken", appTokenId).getFirstAtomicValueWithNameInData("token");
	}

	private boolean userIsActive(DataGroup user) {
		return "active".equals(user.getFirstAtomicValueWithNameInData("activeStatus"));
	}

	private void populateUserRecordTypeNameList() {
		Collection<DataGroup> recordTypes = recordStorage.readList(RECORD_TYPE,
				DataGroup.withNameInData("filter"));

		for (DataGroup recordTypePossibleChild : recordTypes) {
			addChildOfUserToUserRecordTypeNameList(recordTypePossibleChild);
		}
	}

	private void addChildOfUserToUserRecordTypeNameList(DataGroup recordTypePossibleChild) {
		if (isChildOfUserRecordType(recordTypePossibleChild)) {
			addChildToReadRecordList(recordTypePossibleChild);
		}
	}

	protected boolean isChildOfUserRecordType(DataGroup recordTypePossibleChild) {
		return recordHasParent(recordTypePossibleChild)
				&& userRecordTypeIsParentToRecord(recordTypePossibleChild);
	}

	private void addChildToReadRecordList(DataGroup recordTypePossibleChild) {
		String childRecordType = recordTypePossibleChild.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id");
		userRecordTypeNames.add(childRecordType);
	}

	private boolean recordHasParent(DataGroup handledRecordTypeDataGroup) {
		return handledRecordTypeDataGroup.containsChildWithNameInData(PARENT_ID);
	}

	private boolean userRecordTypeIsParentToRecord(DataGroup recordTypePossibleChild) {
		DataGroup parent = recordTypePossibleChild.getFirstGroupWithNameInData(PARENT_ID);
		return "user".equals(parent.getFirstAtomicValueWithNameInData("linkedRecordId"));
	}

	public int getNoOfReadsFromDisk() {
		return noOfReadsFromDisk;
	}

}
