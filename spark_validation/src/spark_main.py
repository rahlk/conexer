'''
This tool runs a given configuration in string representation
and gets its the CPU time performance.
'''
import os
import sys
from os import listdir
from os.path import isfile, join
from spark_profiler import SparkProfiler

perf_dict = dict()
my_profiler = SparkProfiler()
def_conf = 'spark-defaults.conf'


def parse_conf_file(conf_file):
    confs = {}
    with open(conf_file, 'r') as fp:
        for line in fp:
            if line.strip().startswith('#'):
                continue
            ps = line.strip().split(' ')
            ps = [s for s in ps if s.strip() != '']
            if len(ps) == 0:
                continue
            if len(ps) > 2:
                ps = ps[0], ' '.join(ps[1:])
            confs[ps[0]] = ps[1]
    return confs


def_conf_dict = parse_conf_file(def_conf)
def_perf = []
for i in range(2):
    res = my_profiler.profile(i, def_conf_dict)
    def_perf.append(res)
def_perf = sum(def_perf)*1.0/len(def_perf)
print 'default performance:', def_perf
perf_dict['default'] = def_perf


confs_folder = 'validate_confs'
# list all configuration files in confs_folder
all_files = [f for f in listdir(confs_folder) if isfile(join(confs_folder, f))]

for conf_file in all_files:
    if 'default' in conf_file:
        continue
    conf_num = conf_file[4:-5]
    conf = join(confs_folder, conf_file)
    conf_dict = parse_conf_file(conf)
    perfs = []
    for i in range(2):
        res = my_profiler.profile(conf_num, conf_dict)
        perfs.append(res)
    perf = sum(perfs)*1.0/len(perfs)
    print conf_file, '== performance:', perf, '== improvement', (def_perf - perf) * 1.0/def_perf
    perf_dict[conf_file] = perf


# print out
print '================================'
print '================================'
print 'conf, performance, improvement'
perf_tuples = []
def_perf = perf_dict['default']
print 'default performance:', def_perf
del perf_dict['default']
for k, v in perf_dict.iteritems():
    improvement = (def_perf - v)*1.0/def_perf
    perf_tuples.append((k, v, improvement))

sorted_improvement = sorted(perf_tuples, key=lambda tup: tup[2], reverse=True)
for e in sorted_improvement:
    print e[0], ',', e[1], ',', e[2]

print 'Done!'
