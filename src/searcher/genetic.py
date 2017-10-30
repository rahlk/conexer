from profiler import Profiler
from conf_space import ConfSpace
from conf_space import Parameter
from conf_space import ConfDataType
import os
import sys
import random
from util import util
from predict_model import PerfPredict
import time


class Genetic:
    def __init__(self):
        self.conf_space = ConfSpace()
        self.hadoop_semantics = self.conf_space.hadoop_semantics
        self.profiler = Profiler()
        self.predictor = PerfPredict()
        self.max_generation = 50
        self.init_num = 1000
        self.conf_to_profile = 10
        self.profile_num = 1
        # how many configurations to predict with performance predictor before really runs the benchmark
        # self.predict_max = 1000
        self.improvement_threshold = 0.02  # converged if current generation doesn't improve performance by 2%
        return

    def run_vanilla_GA(self):
        '''
        1. get a set of initial configurations (These are parent configurations)
        2. predict/evaluate them for performance (or maybe predict them??)
        3. Find N best predicted configurations to dynamically profile and find the best one
        4. Take these N configurations to evolve (take half of the parameter and exchange and then update M)
        5. get a set of offspring configurations to repeat step 1
        6. Ends till G generations
        :return:
        '''
        best_conf = None
        best_perf = sys.maxsize

        parent_confs = self.get_initial_confs()
        self.generation_index = 1
        self.improvement = sys.maxsize
        best_generation = 1
        # best_dynamic_profile_num = 1
        while self.generation_index < self.max_generation and not self.converged():
            # predict performance of parent configurations
            # pred_parent_perfs = self.predict_N_conf_perf(parent_confs)
            # parent_confs, pred_parent_perfs = self.sort_confs_by_perfs(parent_confs, pred_parent_perfs)

            # select N to dynamically benchmark to select the best one
            # if len(parent_confs) < self.conf_to_profile:
            #     self.conf_to_profile = len(parent_confs)
            # confs_to_profile = random.sample(parent_confs, self.conf_to_profile)
            real_perfs = self.profile_confs(parent_confs)
            # best_dynamic_profile_num += len(confs_to_profile)
            curr_best_conf, curr_best_perf = self.select_top_N_conf_by_perf(parent_confs, real_perfs, 1)
            curr_best_conf, curr_best_perf = curr_best_conf[0], curr_best_perf[0]

            print '=== Best performance in this generation: ', curr_best_perf
            self.improvement = (best_perf - curr_best_perf) / (best_perf*1.0)
            if curr_best_perf <= best_perf:
                best_perf = curr_best_perf
                best_conf = curr_best_conf
                best_generation = self.generation_index

            # now parent configurations are sorted, we select half best ones to evolve
            confs_to_evolve = random.sample(parent_confs, len(parent_confs)/2)
            # add some profiled configurations to evolve
            # sorted_profiled_confs = self.sort_confs_by_perfs(parent_confs, real_perfs)
            # confs_to_evolve.extend(sorted_profiled_confs[:len(confs_to_profile)*3/2])
            if len(confs_to_evolve) == 0:
                break
            # print 'generation', generation_index, 'evovling is finished'
            # parent_confs = self.evolve(best_conf, confs_to_evolve)
            parent_confs = self.evolve(curr_best_conf, confs_to_evolve)
            self.generation_index += 1
        return best_generation, best_conf, best_perf

    def converged(self):
        if self.generation_index <= 1:
            return False
        if self.improvement < self.improvement_threshold:
            return True
        return False

    def run(self):
        '''
        1. get a set of initial configurations (These are parent configurations)
        2. predict/evaluate them for performance (or maybe predict them??)
        3. Find N best predicted configurations to dynamically profile and find the best one
        4. Take these N configurations to evolve (take half of the parameter and exchange and then update M)
        5. get a set of offspring configurations to repeat step 1
        6. Ends till G generations
        :return:
        '''
        best_conf = None
        best_perf = sys.maxsize

        parent_confs = self.get_initial_confs()
        generation_index = 1
        best_generation = 1
        # best_dynamic_profile_num = 1
        while generation_index < self.max_generation:
            # predict performance of parent configurations
            pred_parent_perfs = self.predict_N_conf_perf(parent_confs)
            print 'Generation:', generation_index, '\t=== prediction finished ==='
            parent_confs, pred_parent_perfs = self.sort_confs_by_perfs(parent_confs, pred_parent_perfs)

            # select N to dynamically benchmark to select the best one
            if len(parent_confs) < self.conf_to_profile:
                self.conf_to_profile = len(parent_confs)
            confs_to_profile = random.sample(parent_confs, self.conf_to_profile)
            real_perfs = self.profile_confs(confs_to_profile)
            # best_dynamic_profile_num += len(confs_to_profile)
            curr_best_conf, curr_best_perf = self.select_top_N_conf_by_perf(confs_to_profile, real_perfs, 1)
            curr_best_conf, curr_best_perf = curr_best_conf[0], curr_best_perf[0]

            print 'Best performance in this generation: ', curr_best_perf
            if curr_best_perf <= best_perf:
                best_perf = curr_best_perf
                best_conf = curr_best_conf
                best_generation = generation_index

            # now parent configurations are sorted, we select half best ones to evolve
            confs_to_evolve = random.sample(parent_confs, len(parent_confs)/2)
            # add some profiled configurations to evolve
            sorted_profiled_confs = self.sort_confs_by_perfs(confs_to_profile, real_perfs)
            confs_to_evolve.extend(sorted_profiled_confs[:len(confs_to_profile)*3/2])
            if len(confs_to_evolve) == 0:
                break
            # print 'generation', generation_index, 'evovling is finished'
            # parent_confs = self.evolve(best_conf, confs_to_evolve)
            parent_confs = self.evolve(curr_best_conf, confs_to_evolve)
            generation_index += 1
        return best_generation, best_conf, best_perf

    def evolve(self, best_conf, parents):
        '''
        The evolving step of genetic algorithm.
        :param best_conf:
        :param parents:
        :return:
        '''
        # first find parameters to mutate
        params_to_exchange = random.sample(best_conf.keys(), len(best_conf)/2)
        offspring_confs = []
        for cnf in parents:
            new_cnf = cnf.copy()
            # first exchange
            for p in params_to_exchange:
                new_cnf[p] = best_conf[p]
            # then mutate
            params_to_mutate = random.sample(params_to_exchange, len(params_to_exchange)/2)
            for p in params_to_mutate:
                values = self.conf_space.param_values[p]
                values = [v.value for v in values]
                '''
                This is a new implementation. For numerical parameters, we select values from a range.
                '''
                v = ''
                p_data_type = util.parameters.get(p.lower().strip()).data_type
                if p_data_type in [ConfDataType.float, ConfDataType.integer]:
                    if p_data_type is ConfDataType.float:
                        values = [float(v) for v in values]
                        values = sorted(values)
                        v = random.uniform(values[0], values[-1])
                        v = "{0:.2f}".format(v)
                    else:
                        values = [int(v) for v in values]
                        values = sorted(values)
                        v = random.randint(values[0], values[-1])
                        v = str(v)
                else:
                    v = random.choice(values)
                # print 'mutation step ==== parameter:', p, '==value:', v
                new_cnf[p] = v
            offspring_confs.append(new_cnf)
        # here we remove duplicate configurations by semantics
        offspring_confs = self.hadoop_semantics.remove_dup_confs(offspring_confs)
        return offspring_confs

    def profile_confs(self, confs):
        # print 'Enter profile_confs'
        real_perfs = []
        for index, cnf in enumerate(confs):
            perf = self.profiler.profile(self.profile_num, cnf)
            self.profile_num += 1
            # Chong: print time.strftime("%Y-%d-%m %H:%M:%S"), index+1, 'benchmark done! Performance: ', perf
            real_perfs.append(perf)
        return  real_perfs

    def sort_confs_by_perfs(self, confs, perfs):
        sorted_perfs = sorted(perfs)
        sorted_confs = []
        for i in range(len(sorted_perfs)):
            p_index = perfs.index(sorted_perfs[i])
            sorted_confs.append(confs[p_index])
        return sorted_confs, sorted_perfs

    def select_top_N_conf_by_perf(self, confs, perfs, N):
        if N > len(confs):
            print 'N is larger than the number of configurations'
            return None
        # first sort perf
        tmp_perfs = list(perfs)
        sorted_perfs = sorted(tmp_perfs)
        sorted_confs = []
        for i in range(N):
            p_index = perfs.index(sorted_perfs[i])
            sorted_confs.append(confs[p_index])
        return sorted_confs[:N], sorted_perfs[:N]

    def predict_N_conf_perf(self, confs):
        perfs = []
        for c in confs:
            p = self.predictor.predict(c, c.keys())
            perfs.append(p)
        return perfs

    def get_initial_confs(self):
        '''
        # Initial configurations do not need to consider the hierarchy structure.
        # So we do not need to do that here, only in the evolution steps.
        '''
        #params_to_exploit = self.predictor.important_feature_from_model
        params_to_exploit = util.important_params
        # hierarchy_structure = self.hadoop_semantics.get_partial_structure(params_to_exploit)
        # child_parent = slef.hadoop_semantics.get_parent(params_to_exploit)
        # # here we get parameters that are not children nor parents
        # normal_params = set(params_to_exploit).copy()
        # normal_params.difference_update(set(hierarchy_structure.keys()))
        # normal_params.difference_update(set(child_parent.keys()))
        # normal_params = list(normal_params)
        # params_to_exploit = random.sample(param_to_exploit, len(param_to_exploit)/2)
        conf_set = []
        for i in range(self.init_num):
            new_conf = {}
            # # we need to set the value of parents first and then others
            # # and then normal parameters
            # for p in hierarchy_structure.keys():
            #     values = self.conf_space.param_values.get(p.lower().strip())
            #     random_v = random.choice(values)
            #     new_conf[p] = random_v.value
            # # then get values for children
            # for c, p in child_parent.iteritems():
            #     if p in new_conf and str(new_conf[p]) == 'false':
            #         new_conf[c]
            for p in params_to_exploit:
                values = self.conf_space.param_values.get(p.lower().strip())
                random_v = random.choice(values)
                new_conf[p] = random_v.value
            # print new_conf.values()
            conf_set.append(new_conf)
        conf_set = self.hadoop_semantics.remove_dup_confs(conf_set)
        return conf_set
