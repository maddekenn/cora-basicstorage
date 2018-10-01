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

import se.uu.ub.cora.bookkeeper.data.DataAtomic;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordNotFoundException;
import se.uu.ub.cora.userpicker.UserStorage;

public class UserStorageImp implements UserStorage {

	protected static final String RECORD_TYPE = "recordType";
	private static final String PARENT_ID = "parentId";
	RecordStorageInMemoryReadFromDisk recordStorage;
	private List<String> userRecordTypeNames = new ArrayList<>();
	private final String basePath;
	private int noOfReadsFromDisk = 0;

	public UserStorageImp(Map<String, String> initInfo) {
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

	@Override
	public DataGroup getUserById(String id) {
		DataGroup userDataGroup = findUser(id);
		if (userDataGroup != null) {
			return userDataGroup;
		}
		return repopulateDataFromStorageAndGetUser(id);
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

	private DataGroup repopulateDataFromStorageAndGetUser(String userId) {
		populateFromStorage();
		DataGroup findUser = findUser(userId);
		if (null == findUser) {
			throw new RecordNotFoundException("User not found: " + userId);
		}
		return findUser;
	}

	@Override
	public DataGroup getUserByIdFromLogin(String idFromLogin) {
		try {
			return findUserByIdFromLogin(idFromLogin);
		} catch (Exception e) {
			populateFromStorage();
		}
		return tryAfterRepopulateFromStorageToFindNewlyCreatedUsers(idFromLogin);
	}

	private DataGroup findUserByIdFromLogin(String idFromLogin) {
		Collection<DataGroup> foundUsers = getUserFromStorageByIdFromLogin(idFromLogin);
		throwErrorIfMoreThanOneUserReturnedFromStorage(idFromLogin, foundUsers);
		return foundUsers.iterator().next();
	}

	private Collection<DataGroup> getUserFromStorageByIdFromLogin(String idFromLogin) {
		DataGroup filter = createFilterForIdFromLogin(idFromLogin);
		return recordStorage.readAbstractList("user", filter);
	}

	private void throwErrorIfMoreThanOneUserReturnedFromStorage(String idFromLogin,
			Collection<DataGroup> foundUsers) {
		if (foundUsers.size() > 1) {
			throw new RecordNotFoundException(
					"More than one users with same userId, no user returned: " + idFromLogin);
		}
	}

	private DataGroup createFilterForIdFromLogin(String idFromLogin) {
		DataGroup filter = DataGroup.withNameInData("filter");
		DataGroup part = DataGroup.withNameInData("part");
		filter.addChild(part);
		part.addChild(DataAtomic.withNameInDataAndValue("key", "userId"));
		part.addChild(DataAtomic.withNameInDataAndValue("value", idFromLogin));
		return filter;
	}

	private DataGroup tryAfterRepopulateFromStorageToFindNewlyCreatedUsers(String idFromLogin) {
		return findUserByIdFromLogin(idFromLogin);
	}
}
