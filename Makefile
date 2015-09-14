# scheme2ddl project
#

VERSION := 2.2.4-x2
SRC_FILES := $(shell find src -type f -print)

all : target/scheme2ddl-$(VERSION).jar

target/scheme2ddl-$(VERSION).jar : pom.xml $(SRC_FILES)
	mvn clean install

clean:
	mvn clean
	rm -f dependency-reduced-pom.xml
	rm -rf output

# You need create the "/etc/oracle/tnsnames.ora" file with 'SQL_TEST_HOST' net service name for test by 'make jar-test'
# More info about tnsnames.ora look at http://docs.oracle.com/cd/B28359_01/network.111/b28317/tnsnames.htm
jar-test:
	java -Doracle.net.tns_admin=/etc/oracle -jar target/scheme2ddl-$(VERSION).jar --config src/test/jar_run/scheme2ddl-scott-with-data.config.xml

