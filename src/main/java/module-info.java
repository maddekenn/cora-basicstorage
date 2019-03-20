module se.uu.ub.cora.basicstorage {
	requires se.uu.ub.cora.gatekeeper;
	requires spider;
	requires json;
	requires apptokenstorage;
	requires searchstorage;

	exports se.uu.ub.cora.storage;

	provides se.uu.ub.cora.gatekeeper.user.UserStorageProvider
			with se.uu.ub.cora.storage.OnDiskUserStorageProvider;
}