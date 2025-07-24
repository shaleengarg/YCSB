all:
	mvn -pl site.ycsb:cassandra-binding -am clean package # -DskipTests
	echo "Packaged tarball can be found at cassandra/target/ycsb-cassandra-binding-0.18.0-SNAPSHOT.tar.gz. You can untar this and use its contents for a packaged YCSB."
