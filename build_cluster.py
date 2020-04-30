import os
import shutil
import subprocess
import yaml

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(base_dir, 'conf', 'cluster.yml')) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    if 'servers' not in conf.keys():
        raise ValueError("Cluster config does not contain 'servers'")

    output_dir = os.path.join(base_dir, 'output')
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, 0o755)
    bin_path = os.path.join(output_dir, 'konsen')
    subprocess.check_call(('go', 'build', '-o', bin_path), cwd=base_dir)
    for server_name, endpoint in conf['servers'].items():
        sub_dir = os.path.join(output_dir, server_name)
        os.makedirs(sub_dir, 0o755)

        node_config = conf.copy()
        node_config['localServerName'] = server_name
        with open(os.path.join(sub_dir, 'cluster.yml'), 'w') as f:
            yaml.dump(node_config, f)

        shutil.copy2(bin_path, sub_dir)
        shutil.copy2(os.path.join(base_dir, 'scripts', 'bootstrap.sh'), sub_dir)

    os.remove(bin_path)
