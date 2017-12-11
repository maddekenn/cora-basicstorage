package se.uu.ub.cora.storage;

public class StorageTermData {
	public final String value;
	public final String dataDivider;

	private StorageTermData(String value, String dataDivider) {
		this.value = value;
		this.dataDivider = dataDivider;
	}

	public static StorageTermData withValueAndDataDivider(String value, String dataDivider) {
		return new StorageTermData(value, dataDivider);
	}
}
