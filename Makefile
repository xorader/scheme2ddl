# scheme2ddl project
#

VERSION := 2.2.4-x6
FILE2CELL_JAR := file2cell/file2cell-1.1.jar
SRC_FILES := $(shell find src -type f -print)
JAR_VERSION_IN_POM := $(shell grep '<version>' pom.xml | head -n1 | sed -e 's/.*<version>\(.*\)<\/version>.*/\1/')
DIST_DIR := scheme2ddl-$(VERSION)

all : target/scheme2ddl-$(VERSION).jar $(FILE2CELL_JAR)

target/scheme2ddl-$(VERSION).jar : pom.xml $(SRC_FILES)
	@if [ "$(JAR_VERSION_IN_POM)" != "$(VERSION)" ] ; then echo "The VERSION value in Makefile \"$(VERSION)\" is not equals the version in pom.xml \"$(JAR_VERSION_IN_POM)\". Please fix it." ; exit 1 ; fi
	mvn clean install

$(FILE2CELL_JAR):
	$(MAKE) -C file2cell

clean:
	mvn clean
	rm -f dependency-reduced-pom.xml
	rm -rf output
	$(MAKE) -C file2cell clean

dist: tmp/$(DIST_DIR).tar.gz

tmp/$(DIST_DIR).tar.gz: target/scheme2ddl-$(VERSION).jar ddl2scheme.sh src/main/resources/scheme2ddl*.xml $(FILE2CELL_JAR)
	rm -rf tmp/$(DIST_DIR)
	mkdir -p tmp/$(DIST_DIR)
	cp target/scheme2ddl-$(VERSION).jar tmp/$(DIST_DIR)
	cp ddl2scheme.sh tmp/$(DIST_DIR)
	cp src/main/resources/scheme2ddl*.xml tmp/$(DIST_DIR)
	cp $(FILE2CELL_JAR) tmp/$(DIST_DIR)
	@echo "time java -jar scheme2ddl-$(VERSION).jar --config scheme2ddl-full-schemas-sync.config.xml  -s 'SCOTT' -url "sys as sysdba/syspasswd@localhost:1521:SIDNAME" -o output | tee output/log-scott01" > tmp/$(DIST_DIR)/example-launch.txt
	@echo "./ddl2scheme.sh -v -t sys as sysdba/syspasswd@localhost:1521/SIDNAME" >> tmp/$(DIST_DIR)/example-launch.txt
	cd tmp && tar czf $(DIST_DIR).tar.gz $(DIST_DIR)
	rm -rf tmp/$(DIST_DIR)
	@echo
	@echo "Result dist: tmp/$(DIST_DIR).tar.gz"

# You need create the "/etc/oracle/tnsnames.ora" file with 'SQL_TEST_HOST' net service name for test by 'make jar-test'. Example:
#  SQL_TEST_HOST = (DESCRIPTION =
#    (ADDRESS_LIST =
#      (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.1.3)(PORT = 1521))
#    )
#    (CONNECT_DATA =
#      # SERVICE_NAME must match service_names entry in database
#      (SERVICE_NAME = SOME_SID_NAME)
#    )
#  )
#
# More info about tnsnames.ora look at http://docs.oracle.com/cd/B28359_01/network.111/b28317/tnsnames.htm
jar-test:
	java -Doracle.net.tns_admin=/etc/oracle -jar target/scheme2ddl-$(VERSION).jar --config src/main/resources/scheme2ddl.config.xml

jar-test-full:
	java -Doracle.net.tns_admin=/etc/oracle -jar target/scheme2ddl-$(VERSION).jar --config src/main/resources/scheme2ddl-full-schemas-sync.config.xml

ddl2scheme-test:
	./ddl2scheme.sh -R -i output SCOTT/TIGER@SQL_TEST_HOST2

