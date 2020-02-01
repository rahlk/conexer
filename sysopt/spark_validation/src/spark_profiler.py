'''
This script will profile a Spark configuration and return its execution time.

The entrance function is profile().
'''

from util import make_folder_ready
import subprocess
import os
import time
import sys
from conf import cfg


class SparkProfiler():
    def __init__(self):
        self.config = cfg
        self.itertime = 0
        make_folder_ready(self.config.gen_confs)
        # backup some critical settings
        self.conf_file = os.sep.join([self.config.sys_home, 'conf', 'spark-defaults.conf'])
        self.original_confs = self.parse_orig_confs(self.conf_file)
        self.avg_run_time = 2000    # seconds
        self.hibench_output = ''

    def parse_orig_confs(self, conffile):
        '''
        This function backup the original critical configuration.
        These configurations will be merged with incoming configuration.
        '''
        original_confs = {}
        with open(conffile, 'r') as fp:
            for line in fp:
                if line.strip().startswith('#'):
                    continue
                ps = line.strip().split(' ')
                ps = [s for s in ps if s.strip() != '']
                if len(ps) == 0:
                    continue
                if len(ps) > 2:
                    ps = ps[0], ' '.join(ps[1:])
                original_confs[ps[0]] = ps[1]
        return original_confs

    def profile(self, profile_num, conf):
        '''
        Profile the given configuration and return performance.

        Input: conf: dict{}, a dictionary with parameters as keys and
            parameter values as values.
        '''
        '''
        1. write out configuration file
        2. call HiBench and time the execution
        '''
        # self.itertime += 1
        self.itertime = profile_num
        self.write_into_conf_file(conf)
        exec_time = self.call_benchmark()
        return exec_time

    def write_into_conf_file(self, conf):
        '''
        1. write conf file to log folder: conf_num.conf
        2. write conf file to real spark configuration file
        '''
        tmp_conf = conf.copy()
        # first set critical parameters
        for p, v in self.original_confs.iteritems():
            # if p in tmp_conf:
            #     continue
            tmp_conf[p] = v
        # write to log folder
        log_conf_file = os.sep.join([self.config.gen_confs, 'conf'+str(self.itertime)+'.conf'])
        with open(log_conf_file, 'w') as fp:
            for p, v in tmp_conf.iteritems():
                fp.write(p + ' ' + v + '\n')

        # write into real configuration file
        with open(self.conf_file, 'w') as fp:
            for p, v in tmp_conf.iteritems():
                fp.write(p + ' ' + v + '\n')
        return True

    def call_benchmark(self):
        '''
        Run HiBench and return execution time.
        '''
        cmd = os.sep.join([self.config.hibench_home, 'bin', 'run_all.sh'])
        elapse_times = []
        for i in range(2):
            start = time.time()
            success = self.run_benchmark_cmd(cmd)
            end = time.time()
            execution_time = end-start
            if not success:
                return sys.maxsize
            else:
                elapse_times.append(execution_time)
        return sum(elapse_times)*1.0/len(elapse_times)    # seconds

    def run_benchmark_cmd(self, cmd):
        ret = True
        try:
            # Using PIPE here could cause a deadlock problem.
            # When there is too much content in stdout or stderr,
            # the PIPE will be full and process will hang
            cmd = cmd.split(' ')
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # wait_result = self.wait_timeout(p, self.avg_run_time)
            stdout, stderr = p.communicate()
            return_code = p.returncode
            if return_code == 0:
                self.hibench_output = stderr
            else:
                print 'Run benchmark command failed! Return code is', return_code
                # print stdout
                # print '============='
                # print '============='
                # print stderr
                ret = False
        except Exception as e:
            print 'Profiler: execute CMD failed. Error msg =', e.message
            ret = False
        return ret
