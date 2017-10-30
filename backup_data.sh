today=`date +"%m%d%H%M"`
hdfs dfs -copyToLocal /tmp/hadoop-yarn/staging/history/done_intermediate/root search_data_$today
zip -r hist_data/search_data_$today.zip search_data_$today > /dev/null
hdfs dfs -rm -r /tmp

# back up gen_confs
zip -r hist_data/gen_confs_$today.zip src/gen_confs > /dev/null
rm -r src/gen_confs
