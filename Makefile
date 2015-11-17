# scheme2ddl project
#

VERSION := 2.2.4-x4
SRC_FILES := $(shell find src -type f -print)
JAR_VERSION_IN_POM := $(shell grep '<version>' pom.xml | head -n1 | sed -e 's/.*<version>\(.*\)<\/version>.*/\1/')

all : target/scheme2ddl-$(VERSION).jar

target/scheme2ddl-$(VERSION).jar : pom.xml $(SRC_FILES)
	@if [ "$(JAR_VERSION_IN_POM)" != "$(VERSION)" ] ; then echo "The VERSION value in Makefile \"$(VERSION)\" is not equals the version in pom.xml \"$(JAR_VERSION_IN_POM)\". Please fix it." ; exit 1 ; fi
	mvn clean install

clean:
	mvn clean
	rm -f dependency-reduced-pom.xml
	rm -rf output

# You need create the "/etc/oracle/tnsnames.ora" file with 'SQL_TEST_HOST' net service name for test by 'make jar-test'
# More info about tnsnames.ora look at http://docs.oracle.com/cd/B28359_01/network.111/b28317/tnsnames.htm
jar-test:
	java -Doracle.net.tns_admin=/etc/oracle -jar target/scheme2ddl-$(VERSION).jar --config src/test/jar_run/scheme2ddl-scott-with-data.config.xml

jar-test2:
	java -Doracle.net.tns_admin=/etc/oracle -jar target/scheme2ddl-$(VERSION).jar --config src/test/jar_run/scheme2ddl-scott-load-to-db-supporting.config.xml

