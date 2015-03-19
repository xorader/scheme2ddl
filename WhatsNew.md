## What's new in version 2.3.4 ##
  1. fix [issue #26](https://code.google.com/p/scheme2ddl/issues/detail?id=#26)
  1. add option for replace windows reserved file names, backward compatibility preserved

## What's new in version 2.3.3 ##
  1. fix [issue #23](https://code.google.com/p/scheme2ddl/issues/detail?id=#23) again (regression)


## What's new in version 2.3.2 ##
  1. fix [issue #23](https://code.google.com/p/scheme2ddl/issues/detail?id=#23) sql typo in exporting jobs


## What's new in version 2.3.1 ##
  1. fix [issue #14](https://code.google.com/p/scheme2ddl/issues/detail?id=#14), now proccessing of multy schemas (or foreign schema) is possible with non sysdba connection
  1. fix [issue #19](https://code.google.com/p/scheme2ddl/issues/detail?id=#19)

## What's new in version 2.3 ##
  1. Change skip logic
    1. By default all sql error from oracle skipped and printed in final report
    1. new CLI option `--stop-on-warning`,      stop on getting DDL error
  1. fix [issue #10](https://code.google.com/p/scheme2ddl/issues/detail?id=#10), [issue  #11](https://code.google.com/p/scheme2ddl/issues/detail?id=#11), [issue #13](https://code.google.com/p/scheme2ddl/issues/detail?id=#13)
  1. broke compatibility with old (2.1.X) custom config.
> If you got Exception `"$Proxy12 cannot be cast to com.googlecode.scheme2ddl.UserObjectProcessor"` add new `<bean id="fileNameConstructor">` and `<util:map id="sql_by_default">` to your custom config

## What's new in version 2.2.3 ##
  1. fix [issue #12](https://code.google.com/p/scheme2ddl/issues/detail?id=#12)

## What's new in version 2.2.2 ##
  1. fix [issue #7](https://code.google.com/p/scheme2ddl/issues/detail?id=#7)

## What's new in version 2.2.1 ##

  1. Update spring-batch to 2.2.0.RC1 to fix [issue #6](https://code.google.com/p/scheme2ddl/issues/detail?id=#6) and related [BATCH-1774](https://jira.springsource.org/browse/BATCH-1774)
  1. Compiled for java 1.5
  1. Add retry logic if slow or unstable network connection to oracle
  1. Change logic of processing public database links. If information about public database links not available, continue processing with warning message.


## What's new in version 2.2 ##

  1. Multy schema support (thanks to xorader for initial patch)
    1. `-s` or `-schemas` in command line parameter
    1. `schemaList` in advanced config
    1. support for sysdba connections
  1. New `fileNameConstructor` advanced config section
    1. Construct custom filenames layout from keywords `schema`, `type`, `types_plural`, `object_name`, `ext`ension and them upper case analogues
    1. Rules to map file extension by object type with predefined 'TOAD' or 'PL/SQL Developer' mapping (experimental)
  1. Possibility export CONSTRAINTS and REF\_CONSTRAINTS to separate ddl files
  1. Compatability with old config formats 2.0 and 2.1 preserved, but for multi schema processing use new config format.


## What's new in version 2.1 ##

  1. More verbose error logging with suggestions how to fix error
  1. New object type - `REFRESH_GROUP` (thanks to arnoreinhofer82)
  1. Improved default exclude config - add exlusions of `TABLE PARTITION`, `INDEX PARTITION` and `LOB`
  1. Some new advanced config formatter options
    1. `morePrettyFormat` in ddlFormatter section
    1. `fileNameCase` in writer section NOTE: deprecated in 2.2, use more powerfull `fileNameConstructor` advanced config section


## What's new in version 2.0 ##

  1. oracle-ddl2svn splitted on 2 project, scheme2ddl moved to new namespace under com.googlecode.scheme2ddl
  1. Fully rewritten code
    1. written on top of [spring-batch](http://static.springsource.org/spring-batch/)
    1. scheme2ddl build with [maven](http://maven.apache.org/)
  1. scheme2ddl is now multithreaded
    1. Greatly improved perfomance with `--parallel <number>` option. On my test speed increased in 100 times with 120 threads.
  1. [Configuration](Configuration.md) with xml rewritten:
    1. simplified option names
    1. excluded option types\_for\_getting\_ddl, now scheme2ddl try to unload all user\_objects
    1. 2.x config is not compatible with 1.x
    1. new `--config <path_to_config>` option
  1. fix a lot of issues from 1.x versions
    1. add log4j for logging
    1. fix issues for passing options to dmbs\_metadata in oracle 11g
    1. add sample ant script to oracle-ddl2svn scripts