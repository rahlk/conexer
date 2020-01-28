'''
This tool runs a given configuration in string representation
and gets its the CPU time performance.
'''
import yaml
import os
import sys
from hadoop_profiler import Profiler
import xml.etree.ElementTree as ElementTree

perf_dict = dict()

# my_profiler = Profiler()

important_params = ['dfs.blocksize', 'dfs.replication', 'io.file.buffer.size', 'io.seqfile.compress.blocksize', 'io.seqfile.sorter.recordlimit', 'mapred.child.java.opts', 'mapreduce.input.fileinputformat.split.maxsize', 'mapreduce.input.fileinputformat.split.minsize', 'mapreduce.job.jvm.numtasks', 'mapreduce.job.max.split.locations', 'mapreduce.job.reduce.slowstart.completedmaps', 'mapreduce.job.ubertask.enable', 'mapreduce.job.ubertask.maxmaps', 'mapreduce.job.ubertask.maxreduces', 'mapreduce.map.cpu.vcores', 'mapreduce.map.java.opts', 'mapreduce.map.memory.mb', 'mapreduce.map.output.compress', 'mapreduce.map.output.compress.codec', 'mapreduce.map.sort.spill.percent', 'mapreduce.map.speculative', 'mapreduce.output.fileoutputformat.compress', 'mapreduce.output.fileoutputformat.compress.codec', 'mapreduce.output.fileoutputformat.compress.type', 'mapreduce.reduce.cpu.vcores', 'mapreduce.reduce.input.buffer.percent', 'mapreduce.reduce.java.opts', 'mapreduce.reduce.memory.mb', 'mapreduce.reduce.merge.inmem.threshold', 'mapreduce.reduce.shuffle.input.buffer.percent', 'mapreduce.reduce.shuffle.memory.limit.percent', 'mapreduce.reduce.shuffle.merge.percent', 'mapreduce.reduce.shuffle.parallelcopies', 'mapreduce.shuffle.max.connections', 'mapreduce.shuffle.max.threads', 'mapreduce.task.io.sort.factor', 'mapreduce.task.io.sort.mb', 'mapreduce.tasktracker.http.threads', 'mapreduce.tasktracker.map.tasks.maximum', 'mapreduce.tasktracker.reduce.tasks.maximum', 'yarn.app.mapreduce.am.command-opts', 'yarn.app.mapreduce.am.resource.cpu-vcores', 'yarn.app.mapreduce.am.resource.mb', 'yarn.nodemanager.resource.percentage-physical-cpu-limit', 'yarn.nodemanager.vmem-check-enabled', 'yarn.resourcemanager.scheduler.class', 'yarn.resourcemanager.store.class', 'yarn.scheduler.maximum-allocation-mb', 'yarn.scheduler.maximum-allocation-vcores']

def_str = "{'mapreduce.job.max.split.locations': '10', 'mapreduce.reduce.input.buffer.percent': '0.0', 'mapreduce.output.fileoutputformat.compress': 'false', 'mapreduce.map.speculative': 'true', 'mapreduce.reduce.shuffle.merge.percent': '0.66', 'mapreduce.job.jvm.numtasks': '1', 'yarn.app.mapreduce.am.resource.mb': '2880', 'mapreduce.map.java.opts': '-Xmx1152m', 'mapreduce.input.fileinputformat.split.minsize': '0', 'yarn.scheduler.maximum-allocation-mb': '8192', 'mapreduce.job.ubertask.enable': 'false', 'mapreduce.task.io.sort.factor': '10', 'mapreduce.shuffle.max.connections': '0', 'mapreduce.job.reduce.slowstart.completedmaps': '0.05', 'mapreduce.reduce.shuffle.memory.limit.percent': '0.25', 'mapreduce.output.fileoutputformat.compress.type': 'RECORD', 'mapreduce.reduce.java.opts': '-Xmx2560m', 'dfs.blocksize': '134217728', 'mapreduce.reduce.merge.inmem.threshold': '1000', 'dfs.replication': '1', 'mapreduce.map.sort.spill.percent': '0.80', 'mapred.child.java.opts': '-Xmx200m', 'mapreduce.map.memory.mb': '1024', 'io.file.buffer.size': '65536', 'yarn.resourcemanager.store.class': 'org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore', 'mapreduce.reduce.memory.mb': '1024', 'yarn.nodemanager.vmem-check-enabled': 'true', 'io.seqfile.sorter.recordlimit': '1000000', 'mapreduce.tasktracker.map.tasks.maximum': '2', 'yarn.scheduler.maximum-allocation-vcores': '32', 'mapreduce.output.fileoutputformat.compress.codec': 'org.apache.hadoop.io.compress.DefaultCodec', 'mapreduce.reduce.cpu.vcores': '1', 'mapreduce.map.cpu.vcores': '1', 'yarn.resourcemanager.scheduler.class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler', 'mapreduce.job.ubertask.maxreduces': '1', 'mapreduce.tasktracker.reduce.tasks.maximum': '2', 'mapreduce.map.output.compress.codec': 'org.apache.hadoop.io.compress.DefaultCodec', 'yarn.nodemanager.resource.percentage-physical-cpu-limit': '100', 'mapreduce.shuffle.max.threads': '0', 'mapreduce.map.output.compress': 'false', 'io.seqfile.compress.blocksize': '1000000', 'mapreduce.reduce.shuffle.parallelcopies': '5', 'mapreduce.reduce.shuffle.input.buffer.percent': '0.70', 'mapreduce.job.ubertask.maxmaps': '9', 'mapreduce.tasktracker.http.threads': '40', 'yarn.app.mapreduce.am.resource.cpu-vcores': '1', 'mapreduce.input.fileinputformat.split.maxsize': '268435456', 'mapreduce.task.io.sort.mb': '100', 'yarn.app.mapreduce.am.command-opts': '-Xmx1024m'}"

# def_conf_dict = yaml.load(def_str)
# def_perf = []
# for i in range(2):
#     res = my_profiler.profile(i, def_conf_dict)
#     def_perf.append(res)
# def_perf = sum(def_perf)*1.0/len(def_perf)
# print 'default performance:', def_perf
# perf_dict['default'] = def_perf

def parse_conf_files(folder):
    orig_conf = {}
    hadoop_conf_home = folder
    files = ['mapred-site.xml', 'core-site.xml', 'yarn-site.xml', 'hdfs-site.xml']
    for f in files:
        full_path = hadoop_conf_home + os.sep + f
        root = ElementTree.parse(full_path).getroot()
        properties = root.findall('property')
        for p in properties:
            prop = p.find('name').text
            if prop is None:
                #print 'parsed wrong property'
                continue
            value = p.find('value').text
            if value is None:
                value = ''
            orig_conf[prop] = value
    # filter out useless parameters
    # new_conf = dict()
    # for k in orig_conf:
    #     if k in important_params:
    #         new_conf[k] = orig_conf[k]
    # check the validity of configurations
    valid = True
    if int(orig_conf['mapreduce.map.java.opts'][4:-1]) >= (int(orig_conf['mapreduce.map.memory.mb'])/2048 + 1) * 2048:
        valid = False
    if int(orig_conf['mapreduce.reduce.java.opts'][4:-1]) >= (int(orig_conf['mapreduce.reduce.memory.mb'])/2048 + 1) * 2048:
        valid = False
    print folder, valid
    return orig_conf

# print os.getcwd()

confs_folder = '../validate_confs'
# list all folders in confs_folder
all_confs = next(os.walk(confs_folder))[1]
for c_folder in all_confs:
    conf = confs_folder+os.sep+c_folder
    # best
    best_conf_dict = parse_conf_files(conf)
    # best_conf_dict = yaml.load(best_str)
    # best_perf = []
    # for i in range(2):
    #     res = my_profiler.profile(i, best_conf_dict)
    #     best_perf.append(res)
    # best_perf = sum(best_perf)*1.0/len(best_perf)
    # print c_folder, '==performance:', best_perf, '==improvement', (def_perf-best_perf)*1.0/def_perf
    # perf_dict[c_folder] = best_perf


# print out
# print '================================'
# print '================================'
# print 'conf, performance, improvement'
# perf_tuples = []
# def_perf = perf_dict['default']
# print 'default performance:', def_perf
# del perf_dict['default']
# for k, v in perf_dict.iteritems():
#     improvement = (def_perf - v)*1.0/def_perf
#     perf_tuples.append((k, v, improvement))

# sorted_improvement = sorted(perf_tuples, key=lambda tup: tup[2], reverse=True)
# for e in sorted_improvement:
#     print e[0], ',', e[1], ',', e[2]

print 'Done!'
