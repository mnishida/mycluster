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
from ipyparallel import Client


class TimeoutExpired(Exception):
    pass


class Cluster():

    def __init__(self, hosts):
        if isinstance(hosts, str):
            self.hosts = self.load_hosts(hosts)
        else:
            self.hosts = hosts
        self.num_engines = 0
        self.user = getpass.getuser()
        self.passphrase = getpass.getpass('passphrase: ')
        self.host = socket.gethostname()
        self.ip = socket.gethostbyname(self.host)
        self.bin = f"/home/{self.user}/.envs/cm/bin/"
        self.idfile = f'/home/{self.user}/.ssh/id_rsa'
        self.profile_dir = f"/home/{self.user}/.ipython/profile_default"
        self.ipcontroller = self.bin + 'ipcontroller'
        self.ipengine = self.bin + 'ipengine'
        self.pcontroller = None
        self.engines = []

    def load_hosts(self, hostsfile):
        hosts = []
        with open(hostsfile, "r") as fp:
            for line in fp.readlines():
                host, num, num_threads = line.split()
                hosts.append((host, int(num), int(num_threads)))
        return hosts

    def start_controller(self):
        if self.pcontroller is None:
            cmd = (self.ipcontroller + f" --ip={self.ip}")
            p = pexpect.spawn(cmd, encoding='utf-8')
            p.logfile_read = sys.stdout
            p.expect(r"client::client \[.+\] connected", timeout=60)
            self.pcontroller = p
        else:
            print("ipcontroller is running.")

    def stop_controller(self):
        self.pcontroller.sendintr()

    def copy_controll_files(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        filename = self.profile_dir + "/security/ipcontroller-engine.json"
        for host, num, num_threads in self.hosts:
            if host in [self.host, 'localhost']:
                continue
            ssh.connect(host, username=self.user, password=self.passphrase,
                        key_filename=self.idfile)
            sftp = ssh.open_sftp()
            sftp.put(filename, filename)
            sftp.close()
            ssh.close()
            print(f"{host}: copied controll files")

    def start_engines(self):
        if self.engines:
            print("Engines are running.")
        elif self.pcontroller is None:
            print("Start controller first")
        else:
            self.copy_controll_files()
            self.num_engines = 0
            try:
                for n, (host, num, num_threads) in enumerate(self.hosts):
                    self.num_engines += num
                    s = ("import os; " +
                        f"os.environ.update(OMP_NUM_THREADS=str({num_threads}))")
                    args = rf'--profile-dir={self.profile_dir} -c \"{s}\"'
                    cmd = (f'ssh -i {self.idfile} {self.user}@{host} ' +
                        f'{self.ipengine} {args}')
                    for i in range(num):
                        engine = pexpect.spawn(cmd, encoding='utf-8')
                        engine.logfile_read = sys.stdout
                        engine.expect(rf"Enter passphrase for key '{self.idfile}': ")
                        engine.sendline(self.passphrase)
                        engine.expect(r"Completed registration with id")
                        # time.sleep(0.25)
                        self.engines.append(engine)
                    print(f"{host}: engines started")
                rc = Client(timeout=30)
            except:
                print('Some engines could not start successfully')
                sys.exit()
            count = 1
            while len(rc.ids) != self.num_engines:
                print(f"{len(rc.ids)} engines started")
                time.sleep(2)
                count += 1
                if count > 10:
                    print('{0} engines could not start successfully'.format(
                        self.num_engines - len(rc.ids)))
                    self.shutdown()
                    sys.exit()
            rc.close()
            print(f'A cluster with {self.num_engines} engines started successfully')

    def stop_engines(self):
        for engine in self.engines:
            engine.sendintr()
                
    def _timeout(self):
        raise TimeoutExpired

    def run(self):
        self._loop = asyncio.get_event_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self._ask_exit, signame))
        try:
            self.start_controller()
            self.start_engines()
            self._loop.run_forever()
        finally:
            self.shutdown()
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
        self.stop_engines()
        self.stop_controller()
        print("done.")


def main():
    import os
    import sys
    from optparse import OptionParser
    p = OptionParser()
    p.add_option('--hostfile', action='store',
                 dest='hostfile', default='hosts',
                 type='string', help='Provide a hostfile to use.')
    options, args = p.parse_args()
    filename = options.hostfile
    if not os.path.exists(filename):
        print("hostfile not found")
        sys.exit()
    cluster = Cluster(filename)
    cluster.run()

if __name__ == '__main__':
    main()
