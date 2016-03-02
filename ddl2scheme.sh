#!/bin/sh
#

VERSION="1.1"

##############
## default settings
VERBOSE=0
IS_SINGLE_SCHEME=0
SINGLE_SCHEME_NAME=
IS_MULTY_SCHEME=0
IS_PAUSE_THEN_ERROR=0
IS_SHOW_SPINNER=1
IS_DROP_SCHEME=0
IS_RESTART_DBMS_SCHEDULER=1
IS_DBMS_SCHEDULER_STOPED_ALREADY=0
IS_CREATE_TABLESPACES=0
IS_CREATE_TABLESPACES_ONLY_COMMON=0
IS_ONLY_CHECK_CONNECT=0
IS_ONLY_DROP_SCHEMES=0
IS_ONLY_RECOMPILE_INVALIDS=0
INPUT_DIR="../output"
NLS_LANG="AMERICAN_AMERICA.UTF8"
SQLPLUS_BIN="sqlplus"
SQLPLUS_FIND_DIR="/opt/app/oracle/"
LOG_DIR=
LOG_DIRNAME_DEAFULT="logs_ddl2scheme"
LOG_ALL_ERRORS_LIST="all_list_errors_here.log"
#TMPDIR=/tmp
FILE2CELL_JAR="file2cell/file2cell-1.0.jar"

# Do not touch IT unless you really know what you are doing!!!
# Some special(virtual) types:
# * _TABLESPACES_ - process the '<Input directory>/PUBLIC/TABLESPACES/' directory for CREATE TABLESPACE objects (run process_tablespaces function).
# * _RECOMPILE_ - run the "Recompiling invalid objects" (run recompile_invalid_objects function).
#
CREATE_ORDER_TYPES="_TABLESPACES_,USERS,TYPES,OBJECT_GRANTS,TABLES,SEQUENCES,DB_LINKS,VIEWS,SYNONYMS,FUNCTIONS,PROCEDURES,PACKAGES,JAVA_SOURCES,DATA_TABLES,CONSTRAINTS,_RECOMPILE_,OBJECT_GRANTS,_RECOMPILE_,OBJECT_GRANTS,_RECOMPILE_,INDEXES,REF_CONSTRAINTS,TRIGGERS,JOBS,DBMS_JOBS"

# TODO ???
RE_CREATE_ORDER_TYPES="VIEWS"
# TODO: CLUSTERS type ? подумать что там и зачем

# Terminate script then any errors occurs
set -e

###
# some SQL hacks
##
SQLPLUS_PARSE_ERROR_CODE=33
# remove 'SEGMENT CREATION DEFERRED' from CREATE TABLE constructions.
# Deferred Segment Creation" [ID 887962.1], this feature is part of the Enterprise Edition.
# https://community.oracle.com/thread/1061664
REMOVE_SEGMENT_CREATION_DEFERRED=1

###
# fix dirs paths
PWD_SH=$(dirname -- "$0")
echo "$PWD_SH" | grep -qv "^/" && PWD_SH=`pwd`

# internal values (do not touch)
run_as_sysdba=0

#
error_code_sqlplus_file="${TMPDIR:=/tmp}/ddl2scheme_error_code_sqlplus_file$$.number"
error_code_pause_lock_file="${TMPDIR:=/tmp}/ddl2scheme_error_code_pause_lock_file.lock"
object_grants_processing_counter_file="${TMPDIR:=/tmp}/ddl2scheme_obj_gr_counter_$$.number"
JOB_QUEUE_PROCESSES_DEFAULT=1000
job_queue_processes=$JOB_QUEUE_PROCESSES_DEFAULT


#######################################################################
#
# functions
#
#######################################################################

check_connect ( )
{
	if ! echo -e "select 1 from dual;\nexit;" | $SQLPLUS_BIN -S $CONNECT_URL | perl -pe 'BEGIN { $status=0 } END { exit $status } $status=1 if /^ORA-/; ' > /dev/null ; then
		echo "Connect to DB fail"
		exit 3
	fi
}

fix_sqlplus_bin_path ( )
{
	if which $SQLPLUS_BIN > /dev/null ; then
		# ok
		return
	fi

	if [ $VERBOSE -eq 1 ] ; then
		echo "Can not find ${SQLPLUS_BIN}. Try to find it in the '$SQLPLUS_FIND_DIR'."
	fi

	local new_sqlplus_bin=`find $SQLPLUS_FIND_DIR -path "*/bin/sqlplus" -type f 2>/dev/null || true`
	if [ -z "$new_sqlplus_bin" ] ; then
		echo "Can not find the 'bin/sqlplus' in the '$SQLPLUS_FIND_DIR'. Exit."
		exit 6
	fi
	SQLPLUS_BIN="$new_sqlplus_bin"
}

recompile_invalid_objects ( )
{
	local recompile_count=$1

	local recompile_logfile="${LOG_DIR}/recompile_invalid_objects-${recompile_count}.log"
	local recompile_run_cmd_file="${LOG_DIR}/run_recompile_invalid-${recompile_count}.sql"

	while [ -f $recompile_logfile -o -f $recompile_run_cmd_file ] ; do
		recompile_count=`echo "$recompile_count + 1" | bc`
		recompile_logfile="${LOG_DIR}/recompile_invalid_objects-${recompile_count}.log"
		recompile_run_cmd_file="${LOG_DIR}/run_recompile_invalid-${recompile_count}.sql"
	done

	# usefull links:
	# http://www.orafaq.com/node/2008
	# http://www.dba-oracle.com/t_recompile_recompliling_invalid_objects.htm
	# https://oracle-base.com/articles/misc/recompiling-invalid-schema-objects	-- good example for refactoring
	# http://allineed.ru/ru/oracle-articles/oracle-java/49-auto-recompile-invalid-classes.html -- ещё пример рекомпиляции java class'ов
	# run @$ORACLE_HOME/rdbms/admin/utlrp.sql script by sqlplus on db server ?

	rm -rf $recompile_run_cmd_file
	echo -n "Recompiling invalid objects (logs in '$recompile_logfile')... "
	cat << EOF_RECOMP_SQL | $SQLPLUS_BIN -S $CONNECT_URL > $recompile_logfile
Set heading off;
set feedback off;
SET DOC OFF;
SET TAB OFF;
set echo off;
SET LINESIZE 1000;
SET SERVEROUTPUT ON SIZE 1000000 FORMAT WRAPPED;
set long 50000;
SET PAGESIZE 0;
SET TRIM ON;
set trimspool on;
set verify off;
set termout off;
set embedded on;
set wrap on;

Spool $recompile_run_cmd_file
select 'ALTER ' || object_type || ' ' || owner || '.' || object_name || ' COMPILE;' from all_objects Where status <> 'VALID' And object_type IN ('VIEW','SYNONYM','PROCEDURE','FUNCTION','PACKAGE','TRIGGER') order by object_type, owner, object_name;
select 'ALTER PACKAGE ' || owner || '.' || object_name || ' COMPILE BODY;' from all_objects where status <> 'VALID' And object_type = 'PACKAGE BODY' order by owner, object_name;
select 'ALTER MATERIALIZED VIEW ' || owner || '.' || object_name || ' COMPILE;' from all_objects where status <> 'VALID' And object_type = 'UNDEFINED' order by owner, object_name;
select 'ALTER JAVA CLASS "' || owner || '"."' || object_name || '" RESOLVE;' from all_objects where status <> 'VALID' And object_type = 'JAVA CLASS' order by owner, object_name;
select 'ALTER TYPE ' || owner || '.' || object_name || ' COMPILE BODY;' from all_objects where status <> 'VALID' And object_type = 'TYPE BODY' order by owner, object_name;
Select 'ALTER PUBLIC SYNONYM ' || object_name || ' COMPILE;' from all_objects Where status <> 'VALID' And owner = 'PUBLIC' And object_type = 'SYNONYM' order by owner, object_name;
Spool off;

--set heading on;
--set feedback on;
--set echo on;
exit;
EOF_RECOMP_SQL
	echo "" >> $recompile_logfile
	sed -i $recompile_run_cmd_file -e 's/^SQL> .*//'
	echo "---------------------------------" >> $recompile_logfile
	echo "Running $recompile_run_cmd_file :" >> $recompile_logfile
	echo -e "@${recompile_run_cmd_file}\nexit;" | $SQLPLUS_BIN -S $CONNECT_URL >> $recompile_logfile
	echo "  done"
}

dbms_scheduler_stop ( )
{
	if [ $IS_RESTART_DBMS_SCHEDULER -eq 0 -o $run_as_sysdba -ne 1 -o $IS_DBMS_SCHEDULER_STOPED_ALREADY -eq 1 ] ; then
		return
	fi

	# Executing 'ALTER SYSTEM SET JOB_QUEUE_PROCESSES=0;' does not helps always.
	local sql_get_current_job_queuep_cmd="select value from v\$parameter where name = 'job_queue_processes';"
	echo -e "$sql_get_current_job_queuep_cmd" >> ${LOG_DIR}/dbms_scheduler.log
	echo -e "$sql_get_current_job_queuep_cmd" | $SQLPLUS_BIN -S $CONNECT_URL >> ${LOG_DIR}/dbms_scheduler.log
	job_queue_processes=`echo -e "SET HEADING OFF\nSET FEEDBACK OFF\nSET PAGESIZE 0\nSET SPACE 0\nSET ECHO OFF\n$sql_get_current_job_queuep_cmd" | $SQLPLUS_BIN -S $CONNECT_URL`

	echo "Temporary stop DBMS SCHEDULER (and save the 'job_queue_processes' = '$job_queue_processes')." | tee -a ${LOG_DIR}/dbms_scheduler.log

	local sql_dbms_scheduler_stop_cmd="ALTER SYSTEM SET JOB_QUEUE_PROCESSES=0;\nexec dbms_scheduler.set_scheduler_attribute('SCHEDULER_DISABLED','TRUE');"
	echo -e "$sql_dbms_scheduler_stop_cmd" >> ${LOG_DIR}/dbms_scheduler.log
	echo -e "$sql_dbms_scheduler_stop_cmd\n exit;" | $SQLPLUS_BIN -S $CONNECT_URL >> ${LOG_DIR}/dbms_scheduler.log

	IS_DBMS_SCHEDULER_STOPED_ALREADY=1
}

dbms_scheduler_start ( )
{
	if [ $IS_RESTART_DBMS_SCHEDULER -eq 0 -o $run_as_sysdba -ne 1 ] ; then
		return
	fi

	if [ -z "$job_queue_processes" -o "_$job_queue_processes" = "_0" ] ; then
		job_queue_processes=$JOB_QUEUE_PROCESSES_DEFAULT
	fi

	local sql_dbms_scheduler_start_cmd="ALTER SYSTEM SET JOB_QUEUE_PROCESSES=$job_queue_processes;\nexec dbms_scheduler.set_scheduler_attribute('SCHEDULER_DISABLED','FALSE');"

	echo -e "$sql_dbms_scheduler_start_cmd" >> ${LOG_DIR}/dbms_scheduler.log
	echo -e "$sql_dbms_scheduler_start_cmd\n exit;" | $SQLPLUS_BIN -S $CONNECT_URL >> ${LOG_DIR}/dbms_scheduler.log

	echo "DBMS SCHEDULER started back (and set JOB_QUEUE_PROCESSES to $job_queue_processes)."
}

get_list_of_dirs_by_types_order ( )
{
	local directory=$1

	for sqltype in `echo "$CREATE_ORDER_TYPES" | tr ',' '\n'` ; do
		if [ "$sqltype" = "_RECOMPILE_" -o "$sqltype" = "_TABLESPACES_" ] ; then
			echo "$sqltype"
		else
			find $directory -maxdepth 1 -mindepth 1 -type d -iname "$sqltype" | head -n 1
		fi
	done

	# Take dirs list which not in $CREATE_ORDER_TYPES (and sort by name).
	#  Skip the "logs_ddl2scheme" and the "TABLESPACES" directory too
	local log_defdir_upper=`echo "$LOG_DIRNAME_DEAFULT" | tr '[:lower:]' '[:upper:]'`
	for idir in `find "$directory" -maxdepth 1 -mindepth 1 -type d | sort` ; do
		idir_upper=`basename $idir | tr '[:lower:]' '[:upper:]'`
		if ! echo ",$CREATE_ORDER_TYPES,$log_defdir_upper,TABLESPACES," | grep -q ",$idir_upper," ; then
			echo $idir
		fi
	done
}

extract_scheme_name_from_dir ( )
{
	local dir=$1

	echo "$dir" | sed -r 's#^.*/([^\/]+)/([^\/]+)$#\1#'
}

get_list_of_multydirs_by_types_order ( )
{
	local directory=$1

	for sqltype in `echo "$CREATE_ORDER_TYPES" | tr ',' '\n'` ; do
		if [ "$sqltype" = "_RECOMPILE_" -o "$sqltype" = "_TABLESPACES_" ] ; then
			echo "$sqltype"
		else
			find $directory -maxdepth 2 -mindepth 2 -type d -iname "$sqltype" | sort
		fi
	done

	# Take dirs list which not in $CREATE_ORDER_TYPES (and sort by name).
	#  Skip the "logs_ddl2scheme" and the "TABLESPACES" directory too
	local log_defdir_upper=`echo "$LOG_DIRNAME_DEAFULT" | tr '[:lower:]' '[:upper:]'`
	for idir in `find $directory -maxdepth 2 -mindepth 2 -type d | sort` ; do
		idir_upper=`basename $idir | tr '[:lower:]' '[:upper:]'`
		scheme_name_upper=`extract_scheme_name_from_dir $idir | tr '[:lower:]' '[:upper:]'`
		if [ "$scheme_name_upper" = "$log_defdir_upper" ] ; then
			continue
		fi
		if ! echo ",$CREATE_ORDER_TYPES,$log_defdir_upper,TABLESPACES," | grep -q ",$idir_upper," ; then
			echo $idir
		fi
	done
}

sql_format_header ( )
{
	local type_name=$1

	# К сожалению, так нельзя. Так как в таком случае sqlplus будет прерывать пакетную обработку при первой ошибке.
	# echo "WHENEVER OSERROR EXIT 9;"
	# echo "WHENEVER SQLERROR EXIT SQL.SQLCODE;"

	case "$type_name" in
		VIEWS)
			echo "set sqlblanklines on"
			;;
	esac
	# Sets off the character used to prefix substitution variables. Set the '&' off.
	# ON or OFF controls whether SQL*Plus will scan commands for substitution variables and replace them with
	# their values.
	echo "set define off"
	# Sets the SQL*Plus prefix character. Set the '#' off.
	# While you are entering a SQL command or PL/SQL block, you can enter a SQL*Plus command on a separate line,
	# prefixed by the SQL*Plus prefix character. SQL*Plus will execute the command immediately without
	# affecting the SQL command or PL/SQL block that you are entering. The prefix character must be
	# a non-alphanumeric character.
	echo "set sqlprefix off"
	echo "--"
}

sql_parse_body ( )
{
	local type_name=$1

	# Copy standard input to standard output
	case "$type_name" in
		TABLES)
			if [ $REMOVE_SEGMENT_CREATION_DEFERRED -eq 1 ] ; then
				sed -r "s/(^|[ \t]+)SEGMENT CREATION DEFERRED([ \t]+|$)/\1\2/g"
			else
				cat
			fi
			;;
		*)	# other types
			cat
			;;
	esac

}

sql_format_footer ( )
{
	local type_name=$1

	case "$type_name" in
		DATA_TABLES|DBMS_JOBS)
			echo "commit;"
			;;
	esac
	echo "exit;"
}

sql_finding_errors_in_sqlplus_output ( )
{
	local type_name=$1
	local ora_ignore_addons=""

	if [ $type_name = "INDEXES" ] ; then
		# We ignore errors:
		#   * "ORA-00955: name is already used by an existing object" -- All Primary keys CONSTRAINTS creating has a duplicate UNIQUE INDEX creating.
		#   * "ORA-01408: such column list already indexed" -- All 'CONSTRAINT UNIQUE' (for several columns too) has a duplicate UNIQUE INDEX creating.
		ora_ignore_addons="(?!00955:|01408:"
	fi

	if [ $type_name = "CONSTRAINTS" ] ; then
		# We ignore errors:
		#   * ORA-02329: column of datatype Named Table Type cannot be unique or a primary
		if [ -n "$ora_ignore_addons" ] ; then
			ora_ignore_addons="${ora_ignore_addons}|02329:"
		else
			ora_ignore_addons="(?!02329:"
		fi
	fi

	if [ $type_name = "OBJECT_GRANTS" ] ; then
		# Ignore:
		#  * ORA-01917: user or role 'SOMEUSER' does not exist
		if [ -n "$ora_ignore_addons" ] ; then
			ora_ignore_addons="${ora_ignore_addons}|01917:"
		else
			ora_ignore_addons="(?!01917:"
		fi

		local object_grants_processing_counter=`cat $object_grants_processing_counter_file`
		if [ $object_grants_processing_counter -eq 1 ] ; then
			# Require execute 'OBJECT_GRANTS' two times: after TYPES (before TABLES), and at middle types list.
			# It need for:
			#  - 'GRANT EXECUTE' on some TYPES for nested tables.
			#  - 'GRANT EXECUTE' on some INDEXES
			# First running 'OBJECT_GRANTS' type ignores:
			#  * ORA-00942: table or view does not exist
			#  * ORA-04042: procedure, function, package, or package body does not exist
			ora_ignore_addons="${ora_ignore_addons}|00942:|04042:"
		fi
		if [ $object_grants_processing_counter -lt 3 ] ; then
			# We ignore errors (expect last time):
			#  * ORA-04063: view "*" has errors
			# It need, because view can reffer to other view, that not granted yet and not recompiled.
			ora_ignore_addons="${ora_ignore_addons}|04063:"
		fi
	fi

	if [ -n "$ora_ignore_addons" ] ; then
		ora_ignore_addons="${ora_ignore_addons})"
	fi

	if ! perl -pe "BEGIN { \$status=0 } END { exit \$status } \$status=1 if /^ORA-${ora_ignore_addons}/; \$status=1 if /^SP2-/;" ; then
		# Found some errors.
		if [ ! -f $error_code_sqlplus_file ] ; then
			echo "$SQLPLUS_PARSE_ERROR_CODE" > $error_code_sqlplus_file
		elif [ "`cat $error_code_sqlplus_file`" = "0" ] ; then
			echo "$SQLPLUS_PARSE_ERROR_CODE" > $error_code_sqlplus_file
		fi
	fi
}

sql_final_parse_types_cut ( )
{
	local type_name=$1

	case "$type_name" in
		DATA_TABLES)
			perl -pe 'BEGIN { $row_created_counter=0 } END { print "$row_created_counter row(s) created.\n" } $row_created_counter++ if s/^1 row created.\n$//;'
			;;
		TABLES)
			perl -pe 'BEGIN { $comments_created_counter=0 } END { print "$comments_created_counter comment(s) created.\n" if ($comments_created_counter > 0); } $comments_created_counter++ if s/^Comment created.\n$//;'
			;;
		CONSTRAINTS|REF_CONSTRAINTS)
			perl -pe 'BEGIN { $altered_counter=0 } END { print "$altered_counter table(s) altered..\n" if ($altered_counter > 0); } $altered_counter++ if s/^Table altered.\n$//;'
			;;
		*)	# other types
			cat
			;;
	esac
}

spinner_function_pid=""

start_spinner_process ( )
{
	local type_name=$1
	local spinner_type=0

	if [ "$type_name" != "DATA_TABLES" ] ; then return ; fi

	while true ; do
		case $spinner_type in
		   0) echo -ne "-\b"    ; spinner_type=1 ;;
		   1) echo -ne "\\\\\b" ; spinner_type=2 ;;
		   2) echo -ne "|\b"    ; spinner_type=3 ;;
		   3) echo -ne "/\b"    ; spinner_type=0 ;;
		# realization by perl
		#  0) perl -e 'local $| = 1; print "-\b";'  ; spinner_type=1 ;;
		#  1) perl -e 'local $| = 1; print "\\\b";' ; spinner_type=2 ;;
		#  2) perl -e 'local $| = 1; print "|\b";'  ; spinner_type=3 ;;
		#  3) perl -e 'local $| = 1; print "/\b";'  ; spinner_type=0 ;;
		esac
		sleep 1
	done
}

stop_spinner_process ( )
{
	local type_name=$1
	local spinner_function_pid=$2

	if [ "$type_name" != "DATA_TABLES" ] ; then return ; fi

	if [ $IS_SHOW_SPINNER -eq 1 ] ; then
		kill $spinner_function_pid >/dev/null 2>&1 || true
		trap - 1 2 3 9 15
	fi
}

shutdown_spinner_process ( )
{
	local error_code_file=$1

	trap '' 1 2 3 9 15
	if [ ! -z "$spinner_function_pid" ] ; then
		echo "Cleanup spinner process"
		kill $spinner_function_pid >/dev/null 2>&1 || true
	fi

	rm -rf $error_code_file
	exit 1
}

sql_final_parse_output ( )
{
	local type_name=$1
	local logfile=$2
	local spinner_function_pid=""

	if [ $IS_SHOW_SPINNER -eq 1 -a "$type_name" = "DATA_TABLES" ] ; then
		start_spinner_process $type_name &
		spinner_function_pid=$!
		trap "shutdown_spinner_process $error_code_sqlplus_file" 1 2 3 9 15
	fi

	if [ $VERBOSE -eq 1 ] ; then
		cat | egrep -v "^[ \t]*$" | sql_final_parse_types_cut $type_name | tee -a $logfile
		stop_spinner_process $type_name $spinner_function_pid
		local error_code="`cat $error_code_sqlplus_file 2>/dev/null`"
		if [ "$error_code" != "0" ] ; then
			echo "Some ERROR during sqlplus running. Error code: `cat $error_code_sqlplus_file`" | tee -a $logfile
			if [ $IS_PAUSE_THEN_ERROR -eq 1 ] ; then
				touch $error_code_pause_lock_file
			fi
		fi
	else
		echo -n " "
		cat | egrep -v "^[ \t]*$" |  sql_final_parse_types_cut $type_name >> $logfile
		stop_spinner_process $type_name $spinner_function_pid
		local error_code="`cat $error_code_sqlplus_file 2>/dev/null`"
		if [ "$error_code" != "0" ] ; then
			echo -n "X"
			if [ $IS_PAUSE_THEN_ERROR -eq 1 ] ; then
				touch $error_code_pause_lock_file
				echo
				echo "Some ERROR during sqlplus running. Error code: `cat $error_code_sqlplus_file` (more info in '$logfile')" | tee -a $logfile
			fi
		else
			echo -n "."
		fi
	fi
}

check_tablespace_exist ( )
{
	local tbs_name=$1

	echo "SELECT TABLESPACE_NAME FROM dba_tablespaces WHERE TABLESPACE_NAME = '$tbs_name' ;" | $SQLPLUS_BIN -S $CONNECT_URL | grep "$tbs_name"
}

get_type_tbs_from_line ( )
{
	substring_with_type=`echo "$1" | sed -e 's/^CREATE \(.*\).*TABLESPACE.*/\1/'`
	if [ -n "`echo \"$substring_with_type\" | grep 'TEMPORARY'`" ] ; then
		echo "TEMPORARY"
	elif [ -n "`echo \"$substring_with_type\" | grep 'UNDO'`" ] ; then
		echo "UNDO"
	else
		echo "PERMANENT"
	fi
}

process_tablespaces ( )
{
	local directory=$1

	if [ $IS_MULTY_SCHEME -ne 1 -o $IS_CREATE_TABLESPACES -ne 1 ] ; then
		return
	fi

	# TODO: проверть ещё находиться ли в /PUBLIC/ юзере
	local tbs_dir=`find "$directory" -maxdepth 2 -mindepth 2 -type d -iwholename "*/public/tablespaces" | head -n 1`
	local logfile="${LOG_DIR}/TABLESPACES.log"

	if [ ! -d $tbs_dir ] ; then
		echo "Tablespaces directory not found."
		return
	fi

	for tbs_file in `find "$tbs_dir" -maxdepth 1 -mindepth 1 -type f | sort` ; do
		tbs_create_line=`cat $tbs_file | awk '/CREATE(.*) TABLESPACE "(.*)" (DATAFILE|TEMPFILE)/ { print }' | head -n 1 || true`
		tbs_name=`echo "$tbs_create_line" | sed -e 's/^[^"]*"\(.*\)"[^"]*/\1/'`
		tbs_type=`get_type_tbs_from_line "$tbs_create_line"`

		if [ -z "$tbs_name" ] ; then
			continue
		fi
		if [ -n "`check_tablespace_exist $tbs_name`" ] ; then
			continue
		fi

		if [ $IS_CREATE_TABLESPACES_ONLY_COMMON -eq 0 ] ; then
			echo -n "TABLESPACE $tbs_type '$tbs_name' create as is from '$tbs_file' (log in '$logfile'): "
			echo "Process the '$tbs_file' by sqlplus:" >> $logfile
			if ! ( cat $tbs_file ; echo "exit;" ) | $SQLPLUS_BIN -S $CONNECT_URL \
			  | perl -pe 'BEGIN { $status=0 } END { exit $status } $status=1 if /^ORA-/;' >> $logfile ; then
				echo "FAIL"
			else
				echo "OK"
			fi
			continue
		fi

		if [ "$tbs_type" != "PERMANENT" ] ; then
			echo "Skip creating not PERMANENT tablespace: $tbs_name (with '$tbs_type' type)."
			continue
		fi
		echo -n "TABLESPACE $tbs_type '$tbs_name' create with common options (log in '$logfile'): "
		local tbs_filename=`echo "${tbs_name}.dbf" | tr '[:upper:]' '[:lower:]'`
		local sql="CREATE TABLESPACE \"$tbs_name\" DATAFILE '$tbs_filename' SIZE 536870912 AUTOEXTEND ON NEXT 10485760 MAXSIZE 32767M LOGGING ONLINE EXTENT MANAGEMENT LOCAL AUTOALLOCATE DEFAULT NOCOMPRESS  SEGMENT SPACE MANAGEMENT AUTO;"
		echo "$sql" >> $logfile
		if ! echo -e "$sql\nexit;" | $SQLPLUS_BIN -S $CONNECT_URL \
		  | perl -pe 'BEGIN { $status=0 } END { exit $status } $status=1 if /^ORA-/;' >> $logfile ; then
			echo "FAIL"
		else
			echo "OK"
		fi
	done

}

process_sql_objects_in_dir_type ( )
{
	local idir=$1
	local type_name=$2
	local scheme_name=$3
	local pause_code=$4
	local log_suffix=
	local errors_counter=0

	if [ -n "$scheme_name" ] ; then
		log_suffix=".${scheme_name}"
	else
		log_suffix=""
	fi


	for isqlfile in `find $idir -maxdepth 1 -type f | sort` ; do
		local logfile=
		if [ $type_name = "OBJECT_GRANTS" ] ; then
			logfile="${LOG_DIR}/${type_name}${log_suffix}-`cat $object_grants_processing_counter_file`.log"
		else
			logfile="${LOG_DIR}/${type_name}${log_suffix}.log"
		fi

		# Не нужно дропать, так как dblink'и всё же дропаются вместе со схемой. Если owner dblink'а совпадает, конечно.
		# А если owner не совпадает, то эти dblink'и вообще не влияют на схему и не пересекаются с другими dblink'ами (у которых owner 'SYS', например).
		# А если и хочется дропнуть у нужного owner'а то только вот так: https://adityanathoracledba.wordpress.com/2014/06/29/how-to-drop-other-schemas-database-link-using-sys-user/
		# Пример внизу не сработает (только у SYS удалит).
		#
		#if [ $type_name = "DB_LINKS" -a $IS_DROP_SCHEME -eq 1 ] ; then
		#	local db_link_name=`cat $isqlfile | egrep -i "^[ \t]*CREATE DATABASE LINK " | sed -e 's/^[ \t]*CREATE DATABASE LINK "\([^"]*\)"/\1/i'`
		#	if [ -n "$db_link_name" ] ; then
		#		if [ $VERBOSE -eq 1 ] ; then
		#			echo "DROP DATABASE LINK $db_link_name"
		#		fi
		#		echo "DROP DATABASE LINK $db_link_name" >> $logfile
		#		local sql_drop_db_link_cmd="DROP DATABASE LINK \"$db_link_name\";"
		#		echo "sql> $sql_drop_db_link_cmd" >> $logfile
		#		echo -e "$sql_drop_db_link_cmd\n exit;" | $SQLPLUS_BIN -S $CONNECT_URL >> $logfile
		#	fi
		#fi

		if [ $VERBOSE -eq 1 ] ; then
			echo -e "---\nProcessing $isqlfile" | tee -a $logfile
		else
			echo -e "---\nProcessing $isqlfile" >> $logfile
		fi
		(sql_format_header $type_name; cat $isqlfile | sql_parse_body $type_name; sql_format_footer $type_name) \
			| ( $SQLPLUS_BIN -RESTRICT 3 -S $CONNECT_URL ; echo "$?" >$error_code_sqlplus_file ) \
			| sql_finding_errors_in_sqlplus_output $type_name \
			| sql_final_parse_output $type_name "$logfile"
		local error_code="`cat $error_code_sqlplus_file 2>/dev/null`"
		if [ "$error_code" != "0" ] ; then
			echo "Error: ${error_code}. Sqlfile: '$isqlfile'. Logs: '$logfile'." >> ${LOG_DIR}/$LOG_ALL_ERRORS_LIST
		fi
		rm -f $error_code_sqlplus_file


		if [ "$pause_code" -eq 1 -o -f $error_code_pause_lock_file ] ; then
			rm -rf $error_code_pause_lock_file
			echo "Press ENTER for continue (or CTRL+C for abort) ..."
			read press_enter
		fi
	done
	if [ $VERBOSE -eq 0 ] ; then echo "" ; fi

	if [ $type_name = "OBJECT_GRANTS" ] ; then
		echo "=============================================================" >> $logfile
		echo "=== Processing 'OBJECT_GRANTS' ends. Counter of launching: `cat $object_grants_processing_counter_file`" >> $logfile
		echo "=============================================================" >> $logfile
	fi

	# LOB data files loading
	if [ $type_name = "DATA_TABLES" ] ; then
		local file2cell_option=""
		if [ $IS_SHOW_SPINNER -eq 1 ] ; then
			file2cell_option="-n"
		elif [ $VERBOSE -eq 1 ] ; then
			file2cell_option="-v"
		fi
		if [ -n "$scheme_name" ] ; then
			file2cell_option="$file2cell_option -s $scheme_name"
		elif [ -n "$SINGLE_SCHEME_NAME" ] ; then
			file2cell_option="$file2cell_option -s $SINGLE_SCHEME_NAME"
		fi

		for lobdir in `find $idir -maxdepth 1 -type d | sort` ; do
			local lob_tablename=`basename $lobdir`
			local logfile="${LOG_DIR}/${type_name}${log_suffix}.LOBS.log"
			if [ ! -f $idir/${lob_tablename}.* ] ; then
				continue
			fi
			echo -e "---\nProcessing LOB files in $lobdir" >> $logfile
			echo "-----------------------------" >> $logfile
			(java -Doracle.net.tns_admin=/etc/oracle -Djava.security.egd=file:/dev/./urandom \
				-jar $FILE2CELL_JAR -u "$CONNECT_URL_JAVA" -d "$lobdir" -t $lob_tablename $file2cell_option \
				; echo "$?" >$error_code_sqlplus_file) | tee -a $logfile
			local error_code="`cat $error_code_sqlplus_file 2>/dev/null`"
			if [ "$error_code" != "0" ] ; then
				echo "Error: ${error_code}. LOBDir: '$lobdir'. Logs: '$logfile'." >> ${LOG_DIR}/$LOG_ALL_ERRORS_LIST
			fi
		done
	fi
}

object_grants_counter_increment ( )
{
	local object_grants_processing_counter=`cat $object_grants_processing_counter_file`
	object_grants_processing_counter=`echo "$object_grants_processing_counter + 1" | bc`
	echo $object_grants_processing_counter > $object_grants_processing_counter_file
}

drop_user ( )
{
	local scheme_name=$1

	if [ $run_as_sysdba -ne 1 -o -z "$scheme_name" ] ; then
		return
	fi

	echo -n "Trying to cascade DROP SCHEMA: $scheme_name -" | tee -a ${LOG_DIR}/users_drops.log
	echo "-->" >> ${LOG_DIR}/users_drops.log
	if echo -e "drop user $scheme_name cascade;\nexit;" | $SQLPLUS_BIN -S $CONNECT_URL | perl -pe 'BEGIN { $status=0 } END { exit $status } $status=1 if /^ORA-(?!01918:)/; $status=2 if /^ORA-01918:/; ' >> ${LOG_DIR}/users_drops.log  ; then
		echo " ok"
	else
		result_code="$?"
		if [ "_$result_code" = "_2" ] ; then
			echo " ok (user not found)"
		else
			echo " fail (exit code: $result_code)"
			return $result_code
		fi
	fi
}

drop_all_schemas ( )
{
	local directory=$1
	local log_defdir_upper=`echo "$LOG_DIRNAME_DEAFULT" | tr '[:lower:]' '[:upper:]'`

	for idir in `find "$directory" -maxdepth 1 -mindepth 1 -type d | sort ` ; do
		idir_upper=`basename $idir | tr '[:lower:]' '[:upper:]'`
		if ! echo ",$CREATE_ORDER_TYPES,$log_defdir_upper,TABLESPACES,SYS,PUBLIC," | grep -q ",$idir_upper," ; then
			local schema="$idir_upper"
			local user_dir=`find $idir -maxdepth 1 -mindepth 1 -type d -iname "users"`
			if [ -z "$user_dir" ] ; then
				continue
			fi
			local user_file=`find $user_dir -maxdepth 1 -mindepth 1 -type f -iname "${schema}.*"`
			if [ -z "$user_file" ] ; then
				continue
			fi
			drop_user "$schema"
		fi
	done
}

drop_all_objects_in_current_schema ( )
{
	local logfile="${LOG_DIR}/drop_all_objects.log"

	echo -n "Drop all objects in current schema and exit (logs in '$logfile')... "
	cat << EOF_DROPALL | $SQLPLUS_BIN -S $CONNECT_URL > $logfile
CREATE OR REPLACE
procedure  DROP_ALL_SCHEMA_OBJECTS AS
PRAGMA AUTONOMOUS_TRANSACTION;
cursor c_get_objects is
  select uo.object_type object_type_2,'"'||uo.object_name||'"'||decode(uo.object_type,'TABLE' ,' cascade constraints',null) obj_name2
  FROM USER_OBJECTS uo
  where uo.object_type in ('TABLE','VIEW','PACKAGE','SEQUENCE','SYNONYM', 'MATERIALIZED VIEW', 'FUNCTION', 'PROCEDURE')
        and not (uo.object_type = 'TABLE' and exists (select 1 from user_nested_tables unt where uo.object_name = unt.table_name))
        and not (uo.object_type = 'PROCEDURE' and uo.object_name = 'DROP_ALL_SCHEMA_OBJECTS')
  order by uo.object_type;
cursor c_get_objects_type is
  select object_type, '"'||object_name||'"' obj_name
  from user_objects
  where object_type in ('TYPE');
cursor c_get_dblinks is
  select '"'||db_link||'"' obj_name
  from user_db_links;
cursor c_get_jobs is
  select '"'||object_name||'"' obj_name
  from user_objects
  where object_type = 'JOB';
cursor c_get_dbms_jobs is
  select job obj_number_id
  from user_jobs
  where schema_user != 'SYSMAN';
BEGIN
  begin
    for object_rec in c_get_objects loop
      execute immediate ('drop '||object_rec.object_type_2||' ' ||object_rec.obj_name2);
    end loop;
    for object_rec in c_get_objects_type loop
      begin
        execute immediate ('drop '||object_rec.object_type||' ' ||object_rec.obj_name);
      end;
    end loop;
    for object_rec in c_get_dblinks loop
        execute immediate ('drop database link '||object_rec.obj_name);
    end loop;
    for object_rec in c_get_jobs loop
        DBMS_SCHEDULER.DROP_JOB(job_name => object_rec.obj_name);
    end loop;
    commit;
    for object_rec in c_get_dbms_jobs loop
        dbms_job.remove(object_rec.obj_number_id);
    end loop;
    commit;
  end;
END DROP_ALL_SCHEMA_OBJECTS;

/

execute DROP_ALL_SCHEMA_OBJECTS;
drop procedure DROP_ALL_SCHEMA_OBJECTS;

exit;
EOF_DROPALL
	echo "done"
}

process_all_sql_objects_multy_scheme ( )
{
	local directory=$1
	local previous_type=""

	if [ $IS_DROP_SCHEME -eq 1 ] ; then
		drop_all_schemas $directory
	fi

	for idir in `get_list_of_multydirs_by_types_order "$directory"` ; do
		if [ "$idir" = "_RECOMPILE_" ] ; then
			recompile_invalid_objects 1
			previous_type="$idir"
			continue
		elif [ "$idir" = "_TABLESPACES_" ] ; then
			process_tablespaces "$directory"
			previous_type="$idir"
			continue
		fi

		type_name=`basename $idir | tr '[:lower:]' '[:upper:]'`
		scheme_name=`extract_scheme_name_from_dir $idir`

		if [ "$previous_type" != "$type_name" ] ; then
			if [ "$type_name" = "DBMS_JOBS" -o "$type_name" = "JOBS" ] ; then
				dbms_scheduler_stop
			fi

			if [ "$previous_type" = "OBJECT_GRANTS" ] ; then
				object_grants_counter_increment
			fi

			if [ $VERBOSE -eq 1 ] ; then
				echo "=========================================================================="
				echo -e "\t\tProcess objects of the '$type_name' type in all schemas."
				if [ "$type_name" = "INDEXES" ] ; then
					echo "Don't afraid errors about 'ORA-00955: ...existing object' because all Primary keys CONSTRAINTS creating has a duplicate UNIQUE INDEX creating."
				fi
			else
				echo
				#echo -e "\tProcess objects of the '$type_name' type in all schemas."
			fi
		fi

		#echo "${scheme_name}.${type_name} :: $idir"
		if [ $VERBOSE -eq 1 ] ; then
			echo "------"
			echo -e "\tProcess objects of the '$type_name' type (schema: $scheme_name) in the '$idir' directory."
		else
			echo -en "Process '$type_name' type objects of the '$scheme_name' schema: "
		fi

		#...
		#if [ $VERBOSE -eq 0 ] ; then echo "" ; fi
		process_sql_objects_in_dir_type "$idir" "$type_name" "$scheme_name" 0

		previous_type="$type_name"
	done
}

process_all_sql_objects_single ( )
{
	local directory=$1
	local pause_code=0

	if [ $IS_DROP_SCHEME -eq 1 ] ; then
		if [ $run_as_sysdba -eq 1 ] ; then
			drop_user "$SINGLE_SCHEME_NAME"
		else
			drop_all_objects_in_current_schema
		fi
	fi

	for idir in `get_list_of_dirs_by_types_order "$directory"` ; do
		if [ "$idir" = "_RECOMPILE_" ] ; then
			recompile_invalid_objects 1
			continue
		elif [ "$idir" = "_TABLESPACES_" ] ; then
			process_tablespaces "$directory"
			continue
		fi

		type_name=`basename $idir | tr '[:lower:]' '[:upper:]'`
		# TODO: skip filtered types

		if [ "$type_name" = "DBMS_JOBS" -o "$type_name" = "JOBS" ] ; then
			dbms_scheduler_stop
		fi

		if [ $VERBOSE -eq 1 ] ; then
			echo "=========================================================================="
			echo -e "\tProcess objects of the '$type_name' type in the '$idir' directory."
			if [ "$type_name" = "INDEXES" ] ; then
				echo "Don't afraid errors about 'ORA-00955: ...existing object' because all Primary keys CONSTRAINTS creating has a duplicate UNIQUE INDEX creating."
			fi
		else
			echo -en "Process objects of the '$type_name' type: "
		fi

		# temporary pause -
		# TODO: delete all lines with 'pause_code'
		#if [ "$type_name" = "CONSTRAINTS" ] ; then pause_code=1 ; fi

		process_sql_objects_in_dir_type "$idir" "$type_name" "" $pause_code
		if [ "$type_name" = "OBJECT_GRANTS" ] ; then
			object_grants_counter_increment
		fi
	done
}

extract_user_from_connect_url ( )
{
	local url=$@
	echo "$url" | cut -d "/" -f 1
}

#######################################################################
#######################################################################

##############
## read cmd args and some checks

while [ $# -gt 0 ]
do
	case "$1" in
	  -v) VERBOSE=1 ;;
	  -t) IS_ONLY_CHECK_CONNECT=1 ;;
	  -e) IS_PAUSE_THEN_ERROR=1 ;;
	  -s) IS_SINGLE_SCHEME=1 ; SINGLE_SCHEME_NAME="$2" ; shift ;;
	  -S) IS_SHOW_SPINNER=0 ;;
	  -R) IS_DROP_SCHEME=1 ;;
	  -scheds) IS_RESTART_DBMS_SCHEDULER=0 ;;
	  -stbs) IS_CREATE_TABLESPACES=1 ;;
	  -ctbs) IS_CREATE_TABLESPACES_ONLY_COMMON=1; IS_CREATE_TABLESPACES=1 ;;
	  -onlydropchemas) IS_ONLY_DROP_SCHEMES=1 ;;
	  -onlyrecompileinvalids) IS_ONLY_RECOMPILE_INVALIDS=1 ;;
	  -i) INPUT_DIR="$2"; shift ;;
          -l) LOG_DIR="$2"; shift ;;
	  --) shift; break ;;
	  -*)	echo "usage: $0 [-v] [-e] [-S] [-i <Input directory>] [-l <logdir>] <connect URL>"
		echo "  -v      -- be verbose."
		echo "  -t      -- check connect and exit."
		echo "  -i <input ddls directory> -- directory with DDLs files (generated by scheme2ddl java tool). Default: '$INPUT_DIR'"
		echo "  -s <schemaname>           -- force be 'Single schema loading' for Input directory for 'sys as sysdba' user."
		echo "          Interpret the directory as usual, even if script starting of a superuser."
		echo "          The <schemaname> argument uses then LOBs file loading."
		echo "  -S      -- do not show spinner."
		echo "  -e      -- do pause and wait Enter then errors."
		echo "  -l      -- directory for logs (default: $LOG_DIRNAME_DEAFULT in input directory)."
		echo "  -R      -- do DROP(replace) SCHEMA before (re)creating, then uses 'Multy schema loading' (by superuser)."
		echo "             Or delete all objects for current schema for 'Single schema loading'."
		echo "  -scheds -- do not stop DBMS SCHEDULER during dbms_jobs loading and start after."
		echo "  -stbs   -- do strong (as is) create not existing tablespaces (from PUBLIC/TABLESPACES directory)."
		echo "             Workes for superuser only."
		echo "  -ctbs   -- do common (with common option only) create not existing tablespaces (from same above dir)."
		echo "             Workes for superuser only. This choise creates only PERMANENT tablespaces with common options:"
		echo "               SIZE 512M AUTOEXTEND ... LOGGING...MANAGEMENT..."
		echo "  -onlydropchemas           -- drop all schemas (TODO: and others objects) and exit. List schemas takes from the <Input directory>."
		echo "                               Also, this option can delete all objects for 'Single schema loading'"
		echo "  -onlyrecompileinvalids    -- recompile all invalid objects and exit."
		echo "file2cell version: ..."
		echo "version: $VERSION"
		exit 1
		#TODO: explain about 'Single schema loading' vs 'Multy schema loading'
		;;
	  *) break;;     # terminate while loop
	esac
	shift
done

CONNECT_URL="$@"
CONNECT_URL_JAVA="$CONNECT_URL"

if [ -z "$CONNECT_URL" ]; then
	echo "connect URL empty"
	exit 2
fi

user=`extract_user_from_connect_url $CONNECT_URL`
if [ -n "`echo $user | grep -E ".+as +sysdba *"`" ] ; then
	run_as_sysdba=1
	# convert $CONNECT_URL to correct format for sqlplus
	CONNECT_URL=`(echo -n "$CONNECT_URL" | sed -e 's/ *as *sysdba//') ; echo " as sysdba"`

	if [ $IS_SINGLE_SCHEME -eq 0 ] ; then
		IS_MULTY_SCHEME=1
		if [ $IS_ONLY_CHECK_CONNECT -eq 0 ] ; then
			echo "Run script from superuser, and processing the input directory as multyschema."
		fi
	fi
fi

fix_sqlplus_bin_path

if [ $VERBOSE -eq 1 ] ; then
	echo "Use connect url (for sqlplus): $CONNECT_URL"
	echo "Use input dir: $INPUT_DIR"
	echo "Use sqlplus: $SQLPLUS_BIN"
fi

check_connect "$CONNECT_URL"

if [ $IS_ONLY_CHECK_CONNECT -eq 1 ] ; then
	echo "Connect to the '$CONNECT_URL' - ok. Exit."
	exit 0
fi

rm -rf $error_code_sqlplus_file
rm -rf $error_code_pause_lock_file

if [ ! -z "$NLS_LANG" ] ; then
	export NLS_LANG=$NLS_LANG
fi

if [ -z "$LOG_DIR" ] ; then
	LOG_DIR="$INPUT_DIR/$LOG_DIRNAME_DEAFULT"
fi

# prepare directory for logs
if [ ! -d $LOG_DIR ] ; then
	mkdir -p $LOG_DIR
fi

if [ $IS_ONLY_RECOMPILE_INVALIDS -eq 1 ] ; then
	echo "Recompile invalid objects and exit."
	recompile_invalid_objects 100
	exit 0
fi

if [ $IS_ONLY_DROP_SCHEMES -eq 1 ] ; then
	if [ $IS_MULTY_SCHEME -eq 1 ] ; then
		echo "Drop all schemas and exit (logs in the '${LOG_DIR}/users_drops.log')."
		rm -rf ${LOG_DIR}/users_drops.log
		drop_all_schemas $INPUT_DIR
	else
		if [ $run_as_sysdba -eq 0 ] ; then
			drop_all_objects_in_current_schema
		else
			echo "Error. We do not want to drop all objects in SYS schema :)"
			exit 6
		fi
	fi
	exit 0
fi

rm -rf $LOG_DIR/old
mkdir -p $LOG_DIR/old
mv -f $LOG_DIR/*.* $LOG_DIR/old 2>/dev/null || true
echo "See processing logs in the '$LOG_DIR' directory."

echo "1" > $object_grants_processing_counter_file

#tmp_sql_file=${TMPDIR:=/tmp}/exp2sch_$$.sql
#cat $INPUT_DIR/TABLES/BONUS.sql | sed -r "s/($sed_sql_removes)//g" > $tmp_sql_file
#echo '' | $SQLPLUS_BIN $CONNECT_URL @$tmp_sql_file
#rm $tmp_sql_file

if [ $IS_MULTY_SCHEME -eq 1 ] ; then
	process_all_sql_objects_multy_scheme $INPUT_DIR
else
	process_all_sql_objects_single $INPUT_DIR
fi

dbms_scheduler_start

if [ -f ${LOG_DIR}/$LOG_ALL_ERRORS_LIST ] ; then
	errors_counter=`wc -l ${LOG_DIR}/$LOG_ALL_ERRORS_LIST | cut -f 1 -d " "`
	echo "See all $errors_counter errors list in the '${LOG_DIR}/$LOG_ALL_ERRORS_LIST' file."

	# TODO: exec utl_recomp.recomp_parallel(0); in sqlplus ?
	echo "Also you can run the 'exec utl_recomp.recomp_parallel(0);' in sqlplus (for hand validating and recompiling objects)."
fi

rm -rf $object_grants_processing_counter_file
rm -rf $error_code_sqlplus_file
rm -rf $error_code_pause_lock_file

