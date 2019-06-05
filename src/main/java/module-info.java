module se.uu.ub.cora.basicstorage {
	requires transitive se.uu.ub.cora.gatekeeper;
	requires transitive se.uu.ub.cora.bookkeeper;
	requires se.uu.ub.cora.json;
	requires transitive se.uu.ub.cora.apptokenstorage;
	requires se.uu.ub.cora.searchstorage;
	requires transitive se.uu.ub.cora.spider;
	requires se.uu.ub.cora.logger;

	exports se.uu.ub.cora.basicstorage;

	provides se.uu.ub.cora.gatekeeper.user.UserStorageProvider
			with se.uu.ub.cora.basicstorage.OnDiskUserStorageProvider;
	provides se.uu.ub.cora.apptokenstorage.AppTokenStorageProvider
			with se.uu.ub.cora.basicstorage.OnDiskAppTokenStorageProvider;
}