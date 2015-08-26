# scheme2ddl project
#

SRC_FILES := $(shell find src -type f -print)

all : target/scheme2ddl-2.2.4-SNAPSHOT.jar

target/scheme2ddl-2.2.4-SNAPSHOT.jar : pom.xml $(SRC_FILES)
	mvn clean install

clean:
	mvn clean
	rm dependency-reduced-pom.xml

#.INTERMEDIATE: target

