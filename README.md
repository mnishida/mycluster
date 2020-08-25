# mycluster
mycluster is a utility to start and stop ipcluster using SSH.

## Installation
Clone the 
$ pip install .

## Preparation
### SSH key
Create an rsa key pair without passphrase for local machine in .ssh folder of your home directory (default filename is 'id_rsa_mc', you can specify it as a command line option), and register the public key to the authorized_keys of each remote machine.
You should check that you can SSH login to each host using the rsa key. 

### Create hosts file
Create a file named "hosts" in the current directory. Add a line for each host in your cluster. The line should consists of the hostname, the number of engines, and the number of Open MP threads used by each engine, such as,
> host1 3 2  
> host2 4 3  
> host3 2 2

## Usage
If you use a virtual environment (venv or conda), all the hosts should have the same environment. Make sure that you are in the intended environment, and execute the following command:
> 
> $ mycluster [--id=your_id_file_name]
> 
The cluster starts successfully if you see a message like "A cluster with [num] engines started successfully", where [num] is the total number of running engines.