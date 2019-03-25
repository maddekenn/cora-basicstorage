module se.uu.ub.cora.basicstorage {
	requires transitive se.uu.ub.cora.gatekeeper;
	requires transitive se.uu.ub.cora.bookkeeper;
	requires se.uu.ub.cora.json;
	requires se.uu.ub.cora.apptokenstorage;
	requires se.uu.ub.cora.searchstorage;
	requires transitive se.uu.ub.cora.spider;

	exports se.uu.ub.cora.storage;

	provides se.uu.ub.cora.gatekeeper.user.UserStorageProvider
			with se.uu.ub.cora.storage.OnDiskUserStorageProvider;
}