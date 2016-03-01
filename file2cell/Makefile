#
# This project created by actions:
#  1) mvn archetype:generate -DgroupId=ru.transset.projects.file2cell -DartifactId=file2cell -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
#  2) added the build->plugins->plugin->{artifactId->maven-assembly-plugin ...} section in pom.xml

VERSION := 1.0
PROJECT_NAME := file2cell
RESULT_JAR := $(PROJECT_NAME)-$(VERSION).jar
SRC_FILES := $(shell find src -type f -print)

all: $(RESULT_JAR)

$(RESULT_JAR): target/$(PROJECT_NAME)-$(VERSION)-jar-with-dependencies.jar
	cp target/$(PROJECT_NAME)-$(VERSION)-jar-with-dependencies.jar $(RESULT_JAR)

target/$(PROJECT_NAME)-$(VERSION)-jar-with-dependencies.jar: pom.xml $(SRC_FILES)
	mvn clean package
#	mvn clean compile assembly:single

clean:
	mvn clean
	rm -rf output
	rm -rf $(RESULT_JAR)

# * The '-Djava.security.egd=file:/dev/./urandom' option for java solves trouble with extremly slow connection:
#	- http://stackoverflow.com/questions/5503063/oracle-getconnection-slow
#	- http://stackoverflow.com/questions/137212/how-to-solve-performance-problem-with-java-securerandom
# * The '-Doracle.net.tns_admin=/etc/oracle' option for java require for using SQL_TEST_HOST2 TNS (net service name)
#	from the '/etc/oracle/tnsnames.ora' file.
#	More info about tnsnames.ora look at http://docs.oracle.com/cd/B28359_01/network.111/b28317/tnsnames.htm
jar-test:
	java -Doracle.net.tns_admin=/etc/oracle -Djava.security.egd=file:/dev/./urandom \
		-jar $(RESULT_JAR) \
		-v -u "SCOTT/TIGER@SQL_TEST_HOST2" -f ../output/DATA_TABLES/TABLE_TEST1/LOB_FILE.4.blob_data

