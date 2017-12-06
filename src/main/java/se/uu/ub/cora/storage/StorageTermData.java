package se.uu.ub.cora.storage;

public class StorageTermData {
	public final String value;
	public final String id;
	public final String dataDivider;

	private StorageTermData(String value, String id, String dataDivider) {
		this.value = value;
		this.id = id;
		this.dataDivider = dataDivider;
	}

	public static StorageTermData withValueAndIdAndDataDivider(String value, String id,
			String dataDivider) {
		return new StorageTermData(value, id, dataDivider);
	}
}
