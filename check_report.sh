#!/bin/bash
#title           :check_report.sh
#description     :This script will check volume of a specifuc report in compare to it's related csv files
#author          : Amir Kourdi
#date            :20190416
#version         :0.1
#usage           :bash check_report.sh
# ----------------------------------------- #

# Global Varriables


declare buckets_dir='/data/clearsee_etl/extractworker/'
declare temp_dir='/data/tmp/'
declare IP
declare report_id
declare report_time
#declare target_table


# ----------------------------------------- #


# Script Function

function do_query(){
        /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $1
}


function create_temptable(){
    sql='CREATE TABLE PUBLIC.TEMP (policy_line_key varchar(128), policy_pip_key varchar(128), policy_vc_key varchar(128), volume_in INTEGER, volume_out INTEGER);'
    /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c "$sql" > /dev/null
}


function create_temptable_report(){
    sql='CREATE TABLE PUBLIC.TEMPREPORT (date varchar(128), policy_line_key varchar(128), volume_in varchar(128), volume_out varchar(128));'
    /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c "$sql" > /dev/null
}


function drop_temptable(){
    sql='DROP TABLE PUBLIC.TEMP;'
    /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c "$sql" > /dev/null
}

function drop_temptable_report(){
    sql='DROP TABLE PUBLIC.TEMPREPORT;'
    /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c "$sql" > /dev/null
}




function create_tempfolder(){
	ssh -q clearsee-dw-$1 -C "mkdir -p /data/tmp/"
}


function change_permissions(){
	ssh -q clearsee-dw-$1 -C "chmod 777 /data/tmp/*"
	ssh -q clearsee-dw-$1 -C "chown -R dbadmin:verticadba /data/tmp/"
}

function delete_tempfolder(){
	ssh -q clearsee-dw-$1 -C "rm -rf /data/tmp/"
}


function who_is_primary(){
	IP=$(PGPASSWORD=dbadmin /usr/bin/psql -h clearsee-bi-ipv -d central_repository -U dbadmin -c "select external_ip from etl.nodes where is_primary ='t';" |  sed -n '3p' | sed 's/ //g' | tail -1)
}

function cmd_dw_primary(){
	ssh $IP -q -C $1
}
# ----------------------------------------- #




# MAIN
main () 
{

	#printf 'Enter Report Path: '
	#read path
	path=$1
	create_temptable_report 1
	create_temptable 1
	for j in {1..3}
	do
		create_tempfolder $j

		ssh -q clearsee-dw-$j -C "scp -q clearsee-bi-ipv:$path /data/tmp/report.csv.gz"
		ssh -q clearsee-dw-$j -C "gunzip /data/tmp/report.csv.gz"

		report_id=$(echo $path | cut -d '/' -f4)
		report_time=$(echo $path | cut -d '/' -f6 |cut -d '-' -f4 | cut -d '.' -f1)
		Y=${report_time:0:4}
		M=${report_time:4:2}
		D=${report_time:6:2}
		h=${report_time:8:2}
		m=${report_time:10:2}
		sql_date="'$Y-$M-$D $h:$m:00'"


		COUNTER=1
		for i in $(ssh -q clearsee-dw-$j -C "ls $buckets_dir | grep $report_time")
		do
		full_path_bucket=$(echo $buckets_dir$i | tr -d '[:space:]')
		tmp_csv=$(echo $temp_dir$i | tr -d '[:space:]' | tr '\n' ' ' )
		csv_file=$(echo "${tmp_csv::-3}")
		new_format_csv="'$csv_file'"
		ssh -q clearsee-dw-$j -C "gunzip -c $full_path_bucket >  $csv_file"
		new_name=$temp_dir'bucket_'$COUNTER.csv
		ssh -q clearsee-dw-$j -C "cut -d, -f7-9,32-33 $csv_file > $new_name"
		ssh -q clearsee-dw-$j -C "rm -rf $temp_dir*CONV*"
		change_permissions $j
		query="\"COPY PUBLIC.TEMP FROM LOCAL '$new_name' ENCLOSED BY '^' DELIMITER ',' NULL '' SKIP 1 ABORT ON ERROR TRAILING NULLCOLS;\""
		ssh -q clearsee-dw-$j -C /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $query > /dev/null

		COUNTER=$[$COUNTER +1]
		done
	done

	query="\"COPY PUBLIC.TEMPREPORT FROM LOCAL '/data/tmp/report.csv' ENCLOSED BY '^' DELIMITER ',' NULL '' ABORT ON ERROR TRAILING NULLCOLS;\"" 
	ssh -q clearsee-dw-1 -C /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $query  > /dev/null

	echo ' '
	echo "============================================================================================================================================================"
	echo " Sum Volume Result From Buckets That Related To Report ID: $report_id From Date: $report_time Local File: $path"
	echo "============================================================================================================================================================"

	query="\"SELECT '$report_id' as REPORT_ID , cast(round(sum(volume_in)/1024/1024/1024,2) as float) as IN_VOLUME_GB, cast(round(sum(volume_out)/1024/1024/1024,2) as float) AS OUT_VOLUME_GB, cast(round(sum(volume_in+volume_out)/1024/1024/1024,2) as float) as TOATAL_VOLUME_GB from public.temp where policy_line_key ilike '%$report_id%' group by 1 order by 1;\""
	ssh -q clearsee-dw-$j /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $query



	echo ' '
	echo "============================================================================================================================================================="
	echo " Sum Volume Result From 'PROD.DWH_FACT_KDDI_CONV_RAW' TABLE For Data That Relevent To Report_ID : $report_id From Date: $report_time"
	echo "============================================================================================================================================================="


	query="\"SELECT '$report_id' as REPORT_ID, cast(round(sum(volume_in)/1024/1024/1024,2) as float) as IN_VOLUME_GB, cast(round(sum(volume_out)/1024/1024/1024,2) as float) as OUT_VOLUME_GB, cast(round(sum(volume_in+volume_out)/1024/1024/1024,2) as float) as TOTAL_VOLUME_GB from prod.dwh_faCT_kDDI_CONV_RAW where policy_line_key ilike '%$report_id%' and period_min_key=$sql_date ;\""
	ssh -q clearsee-dw-1 -C /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $query

	echo ' '
	echo "========================================================================================================"
	echo " Sum Volume Result From Original Report ID: $report_id From Date: $report_time"
	echo "========================================================================================================"
	query="UPDATE public.tempreport SET volume_in = REPLACE(volume_in, '\"', ''), volume_out = REPLACE(volume_out, '\"', '');commit;"
	/opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c "$query"  > /dev/nul

	query="\"SELECT '$report_id' as REPORT_ID, cast(round(sum(volume_in::NUMERIC)/1024/1024/1024,2) as float) as IN_VOLUME_GB, cast(round(sum(volume_out::NUMERIC)/1024/1024/1024,2) as float) as OUT_VOLUME_GB, cast(round(sum(volume_in::NUMERIC+volume_out::NUMERIC)/1024/1024/1024,2) as float) as TOTAL_VOLUME_GB from public.tempreport;\""
	ssh -q clearsee-dw-1 -C /opt/vertica/bin/vsql -U dbadmin -w dbadmin -d clearseedwh -c $query

	echo ' '
	echo "========================================================================================================"
	echo " Sum of Accepted and Rejected Rows During Loading Buckets in: $sql_date"
	echo "========================================================================================================"
	query="select (CASE WHEN sum(accepted_rows) is NULL THEN 0 ELSE sum(accepted_rows) END) as ACCEPTED, (CASE WHEN sum(rejected_rows) is NULL THEN 0 ELSE sum(rejected_rows) END) as REJECTED from etl.files where original_file_name ilike '%$report_time%';"
	PGPASSWORD=dbadmin /usr/bin/psql -h clearsee-bi-ipv -d central_repository -U dbadmin -c "$query"




	for j in {1..3}
	do 	
	delete_tempfolder $j
	done

	drop_temptable 1
	drop_temptable_report 1



}


case $1 in
  '-p'|'-path')
     main $2
     ;;
esac
