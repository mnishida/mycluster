# -*- coding: utf-8 -*-
__author__ = "Munehiro Nishida"
__version__ = "0.1.0"
__license__ = "GPLv3"


import pexpect
import sys
import time
import socket
import getpass
import signal
import asyncio
import functools
import paramiko
import ipyparallel as ipp


class Cluster():

    def __init__(self, hosts, id='id_rsa_mc', log=False, nodb=True):
        if isinstance(hosts, str):
            self.hosts = self.load_hosts(hosts)
        else:
            self.hosts = hosts
        self.num_engines = 0
        self.user = getpass.getuser()
        # self.passphrase = getpass.getpass('passphrase: ')
        self.host = socket.gethostname()
        self.ip = socket.gethostbyname(self.host)
        self.bin = f"{sys.prefix}/bin/"
        self.id_file = f'/home/{self.user}/.ssh/{id}'
        self.profile_dir = f"/home/{self.user}/.ipython/profile_default"
        self.ipcluster = self.bin + 'ipcluster'
        self.ipcontroller = self.bin + 'ipcontroller'
        self.ipengine = self.bin + 'ipengine'
        self.engines = []
        self.log = log
        self.nodb = nodb
        self.start()

    def __call__(self):
        try:
            self.run()
        finally:
            self.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def load_hosts(self, hostsfile):
        hosts = []
        with open(hostsfile, "r") as fp:
            for line in fp.readlines():
                host, num, num_threads = line.split()
                hosts.append((host, int(num), int(num_threads)))
        return hosts

    def start_cluster(self):
        if not hasattr(self, 'pcluster'):
            cmd = (self.ipcluster + f' start -n 0 --ip={self.ip}'
                   ' --IPClusterEngines.engine_launcher_class=SSH'
                   ' --SSHEngineSetLauncher.delay=0.0'
                   ' --IPClusterStart.delay=0.0'
                   ' --LocalEngineSetLauncher.delay=0.0')
            if self.nodb:
                cmd += ' -- --nodb'
            p = pexpect.spawn(cmd, encoding='utf-8')
            if self.log:
                p.logfile_read = sys.stdout
            # p.expect("Engines appear to have started successfully",
            #             timeout=60)
            p.expect("Starting  engines with", timeout=30)
            time.sleep(2)
            setattr(self, 'pcluster', p)
        else:
            print("ipcluster is running.")

    def start_controller(self):
        if not hasattr(self, 'pcontroller'):
            cmd = (self.ipcontroller + f' --ip={self.ip}')
            if self.nodb:
                cmd += ' --nodb'
            p = pexpect.spawn(cmd, encoding='utf-8')
            if self.log:
                p.logfile_read = sys.stdout
            p.expect(r"client::client \[.+\] connected", timeout=60)
            setattr(self, 'pcontroller', p)
        else:
            print("ipcontroller is running.")

    def copy_controll_files(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        filename = self.profile_dir + "/security/ipcontroller-engine.json"
        for host, num, num_threads in self.hosts:
            if host in [self.host, 'localhost']:
                continue
            # ssh.connect(host, username=self.user, password=self.passphrase,
            #             key_filename=self.id_file)
            ssh.connect(host, username=self.user, key_filename=self.id_file)
            sftp = ssh.open_sftp()
            sftp.put(filename, filename)
            sftp.close()
            ssh.close()
            print(f"{host}: copied controll files")

    def start_engines(self):
        if self.engines:
            print("Engines are running.")
        # elif not hasattr(self, 'pcontroller'):
        #     print("Start controller first")
        elif not hasattr(self, 'pcluster'):
            print("Start cluster first")
        else:
            self.copy_controll_files()
            self.num_engines = 0
            try:
                for n, (host, num, num_threads) in enumerate(self.hosts):
                    self.num_engines += num
                    s = ("import os; " +
                         f"os.environ.update(OMP_NUM_THREADS=str({num_threads}))")
                    args = r'--profile-dir={0} -c \"{1}\"'.format(
                        self.profile_dir, s)
                    cmd = (f'ssh -i {self.id_file} {self.user}@{host} ' +
                           f'{self.ipengine} {args} &')
                    for i in range(num):
                        engine = pexpect.spawn(cmd, encoding='utf-8')
                        if self.log:
                            engine.logfile_read = sys.stdout
                        # engine.expect(rf"Enter passphrase for key '{self.id_file}': ")
                        # engine.sendline(self.passphrase)
                        engine.expect(r"Completed registration with id")
                        self.engines.append(engine)
                    print(f"{host}: engines started")
            except (pexpect.EOF, pexpect.TIMEOUT):
                print('Some engines could not start successfully')
                sys.exit()
            count = 1
            num_started = 0
            try:
                self.rc = ipp.Client(timeout=30)
                num_started = len(self.rc.ids)
                while num_started != self.num_engines:
                    print(f"{num_started} engines running")
                    time.sleep(3)
                    count += 1
                    if count > 10:
                        raise TimeoutError
                    num_started = len(self.rc.ids)
            except (TimeoutError, ipp.error.TimeoutError):
                print('{0} engines could not start successfully'.format(
                    self.num_engines - num_started))
                sys.exit()
            print(f'A cluster with {self.num_engines} engines started successfully')

    def start(self):
        # self.start_controller()
        self.start_cluster()
        self.start_engines()

    def _timeout(self):
        raise TimeoutError

    def run(self):
        self._loop = asyncio.get_event_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self._ask_exit, signame))
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def _ask_exit(self, signame):
        signal.signal(signal.SIGALRM, self._timeout)
        signal.alarm(5)
        ans = None
        try:
            ans = input("Shutdown this cluster (y/[n])?")
        except Exception:
            pass
        finally:
            signal.alarm(0)
        if ans == 'y':
            print("Shutdown confirmed")
            self._loop.stop()
        elif ans is None:
            print("\nNo answer for 5s: resuming operation...")
        else:
            print("resuming operation...")

    def shutdown(self):
        print("Shutting down ...", end='')
        if self.engines:
            for engine in self.engines:
                engine.sendintr()
            self.engines.clear()
        if hasattr(self, 'rc'):
            self.rc.shutdown(hub=True)
            delattr(self, 'rc')
        # if hasattr(self, 'pcontroller'):
        #     self.pcontroller.sendintr()
        #     delattr(self, 'pcontroller')
        if hasattr(self, 'pcluster'):
            self.pcluster.sendintr()
            # self.pcluster.expect(r"Removing pid file:")
            self.pcluster.expect([r"Stopping engine(s)", pexpect.EOF], timeout=30)
            delattr(self, 'pcluster')
        print("done.")


def main():
    import os
    import sys
    from optparse import OptionParser
    p = OptionParser()
    p.add_option('--hostfile', action='store',
                 dest='hostfile', default='hosts',
                 type='string', help='Provide a hostfile to use.')
    p.add_option('--id', action='store',
                 dest='id', default='id_rsa_mc',
                 type='string', help='Provide a rsa identity file to use.')
    p.add_option('-v', action='store_true', dest='verbose',
                 help='Display log')
    options, args = p.parse_args()
    filename = options.hostfile
    if not os.path.exists(filename):
        print("hostfile not found")
        sys.exit()
    if options.verbose:
        log = True
    else:
        log = False
    with Cluster(filename, options.id, log) as cluster:
        cluster.run()

if __name__ == '__main__':
    main()
