import subprocess

hibench_home = '/root/sysopt/HiBench/'

cmd = 'bash ' + hibench_home + 'bin/run_all.sh'
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
std_output, err_output = p.communicate()
print '===============STD==========='
print std_output
print '===============ERR==========='
print err_output
