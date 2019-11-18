package se.uu.ub.cora.basicstorage;

import se.uu.ub.cora.data.DataAtomic;
import se.uu.ub.cora.data.DataAtomicFactory;

public class DataAtomicFactorySpy implements DataAtomicFactory {

	@Override
	public DataAtomic factorUsingNameInDataAndValue(String nameInData, String value) {
		return new DataAtomicSpy(nameInData, value);
	}

	@Override
	public DataAtomic factorUsingNameInDataAndValueAndRepeatId(String nameInData, String value,
			String repeatId) {
		return new DataAtomicSpy(nameInData, value);
	}

}
