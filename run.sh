#cp /usr/lib/hadoop/etc/hadoop/core-site.xml original_confs
#cp /usr/lib/hadoop/etc/hadoop/mapred-site.xml original_confs
#cp /usr/lib/hadoop/etc/hadoop/yarn-site.xml original_confs
#cp /usr/lib/hadoop/etc/hadoop/hdfs-site.xml original_confs


#sudo cp hadoop_conf/* /usr/lib/hadoop/etc/hadoop
cd src
python -u main.py 300
