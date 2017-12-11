import os

"""
utility function for constructing docker args
when launching a cmd inside a docker
"""


class DockerArgs:

    def __init__(self, docker, cmd):
        self._docker = docker
        self._args = {}
        self._d = {}
        self._cmd = cmd

    def get_docker_dirs(self):
        return [(root, volume) for root, path, volume in self._d.values()]

    def get_real_path(self, argname):
        tup = self._d.get(argname, None)
        return None if tup is None else os.path.join(tup[0], tup[1])

    def get_docker_path(self, argname):
        tup = self._d.get(argname, None)
        return None if tup is None else os.path.join(tup[2], tup[1])

    def add_path(self, argname, root, path, volume):
        """
        if argname startswith '-' or '--' the path arguments will be included
        as a command argument
        """
        for _root, _path, _volume in self._d.values():
            if volume == _volume:
                raise Exception("volume already exists", volume)
        self._d[argname] = (root, path, volume)

    def add_arg(self, argname, argvalue):
        self._args[argname] = argvalue

    def get_docker_args(self):
        # docker command
        CMD = ["docker", "run"]
        # volumes
        for path, volume in self.get_docker_dirs():
            CMD.extend(['-v', '{}:{}'.format(path, volume)])
        CMD.append(self._docker)
        # command
        CMD.extend(self._cmd)
        # path command args
        for argname in self._d:
            if argname[:1] == '-':
                CMD.extend([argname, self.get_docker_path(argname)])
        # other command args
        for argname in self._args:
            CMD.extend([argname, self._args[argname]])
        return CMD


if __name__ == '__main__':

    # example usage
    da = DockerArgs('dockername', ['python', 'program.py'])
    da.add_path('--input', '/path/to/input_dir', 'input_file', '/input')
    da.add_path('--output', '/path/to/output_dir', 'output_file', '/output')
    da.add_path('resource', '/path/to/resource_dir', 'resource_file', '/resource')
    da.add_arg('--arg', 'value')

    args = da.get_docker_args()
    print(args)

    """
    ['docker', 'run',
    '-v', '/path/to/input_dir:/input',
    '-v', '/path/to/resource_dir:/resource',
    '-v', '/path/to/output_dir:/output',
    'dockername',
    'python', 'program.py',
    '--input', '/input/input_file',
    '--output', '/output/output_file',
    '--arg', 'value']
    """
