'''
This script implements a pure random algorithm for configuration space exploration.

Now the stop criterion becomes "maximum number of iterations".
'''
import os
import sys
import random
import time
import math  # for math.exp()
import numpy as np
from util import util
from conf_space import ConfDataType
from abs_classes import AbstractAlgo
from hadoop import HadoopConfChecker


class RandomExplore(AbstractAlgo):
    def __init__(self, conf, confspace, profiler):
        self.conf_space = confspace()
        # self.hadoop_semantics = self.conf_space.hadoop_semantics
        self.profiler = profiler()
        self.type_checker = HadoopConfChecker()
        self.type_checker.set_all_param_value(self.conf_space.param_values)
        self.curr_genconf_folder = conf.gen_confs + os.sep + 'conf'
        # the maximum generations to evolve, as a stopping criterion
        # self.max_generation = 10
        # the population size in each generation
        # self.population_size = 200
        # self.conf_to_profile = 10
        self.profile_num = 0
        # the global performance improvement threshold, as a stopping criterion
        self.global_improvement = 0.8
        # discard a candidate if its improvement is lower than this threshold
        # self.local_improvement = 0.01     # MCMC decides which configuration enter into parent set
        self.max_iter = 6000    # the maximum number of iteration for MCMC
        self.invalid_confs = []

    def run(self):
        '''
        This is the entrance.
        '''
        params = self.conf_space.get_all_params()
        default_conf = self.conf_space.get_default_conf(params)
        print 'default configuration:'
        print default_conf
        default_perf = self.profile_conf(default_conf)
        if default_perf == sys.maxsize:
            print 'profile default configuration failed, exit...'
            sys.exit(-1)
        print 'Default performance. Profile num:', self.profile_num - \
            1, '=== Performance:', default_perf
        best_conf = default_conf.copy()
        best_perf = default_perf
        best_profile_num = self.profile_num - 1
        for _ in xrange(self.max_iter):
            # random select a configuration and profile it
            conf_to_profile = self.get_a_random_conf()
            curr_perf = self.profile_conf(conf_to_profile)

            if curr_perf <= best_perf:
                print 'Better performance found. Profile num:', self.profile_num - \
                    1, '=== Performance:', curr_perf
                # update best configuration and performance
                best_perf = curr_perf
                best_conf = conf_to_profile
                best_profile_num = self.profile_num - 1

        print 'invalid confs:', self.invalid_confs
        print 'Best performance:', best_perf, '==== best iteration:', best_profile_num
        return best_profile_num, best_conf, best_perf

    def get_a_random_conf(self):
        params_to_exploit = self.conf_space.get_all_params()
        conf = {}
        for p in params_to_exploit:
            values = self.conf_space.get_values_by_param(p.lower().strip())
            # random_v = self.get_random_value(p, values)
            random_v = random.choice(values).value
            conf[p] = random_v
        return conf

    def get_random_value(self, p, values):
        p_data_type = util.parameters.get(p.lower().strip()).data_type
        values = [v.value for v in values]
        if p_data_type in [ConfDataType.float, ConfDataType.integer]:
            if p_data_type is ConfDataType.integer:
                values = [int(v) for v in values]
                minval, maxval = min(values), max(values)
                randomVal = random.randint(minval, maxval)
                return randomVal
            elif p_data_type is ConfDataType.float:
                values = [float(v) for v in values]
                minval, maxval = min(values), max(values)
                randomVal = random.uniform(minval, maxval)
                return randomVal
        return random.choice(values)

    def profile_conf(self, conf):
        # print 'Enter profile_confs'
        perf = sys.maxsize
        # type checker for cnf
        if not self.type_checker.check(self.profile_num, conf):
            print 'type-checking failed, config', str(self.profile_num)
            self.invalid_confs.append(self.profile_num)
            genconf_folder = self.curr_genconf_folder + str(self.profile_num)
            util.make_folder_ready(genconf_folder)
            tmp_conf = conf.copy()
            for p, v in self.profiler.original_confs.iteritems():
                tmp_conf[p] = v
            util.write_into_conf_file(tmp_conf, genconf_folder)
            self.profile_num += 1
            return perf
        perf = self.profiler.profile(self.profile_num, conf)
        print time.strftime(
            "%Y-%d-%m %H:%M:%S"), self.profile_num, 'benchmark done! Performance: ', perf
        self.profile_num += 1
        return perf
