import sys

sys.path.append('lib')

from cluster import start_cluster
from sandbox import CONFIG, figure_out_binary

figure_out_binary()

nodes = start_cluster(1, 0, 1, CONFIG, [["epoch_length", 10]], {})
status = nodes[0].get_status()
print(status)
res = nodes[0].json_rpc('sandbox_produce_blocks', {'num_blocks': 1000}, timeout=30)
print('-----')
assert 'result' in res, res

status = nodes[0].get_status()

print(status)