import sys

def total_count(d):
    return sum([d[key] for key in d.keys()])

def fail_rate(a, b):
    return str(round(a / b * 100, 2)) + "% " + str(a)

def get_top_10(d):
    total = total_count(d)
    cnt_and_value = sorted([(d[key], key, str(round(d[key] / total * 100, 2)) + "%") for key in d.keys()])
    cnt_and_value.reverse()
    return cnt_and_value[:5]

ok, fail = 0, 0
current_cost = 16101955926
new_cost =    10000000000 * int(sys.argv[2])
tgas_3 =     3000000000000
tgas_100 = 100000000000000
new_failures, new_failures_with_extra_100tgas, new_failures_with_extra_200tgas = dict(), dict(), dict()
total_blocks = 0
with open(sys.argv[1]) as f:
    for line in f:
        if not line.startswith("fn_fail") and not line.startswith("fn_ok"):
            if "blocks" in line:
                total_blocks = int(line.split(' ')[1])
            continue

        tokens = line.rstrip().split(' ')
        if tokens[0] == "fn_fail":
            fail += 1
            continue
        ok += 1

        real_reads = int(tokens[1])
        remaining_gas = int(tokens[4]) - int(tokens[3]) - tgas_3
        account_id = tokens[5]

        extra = real_reads * (new_cost - current_cost)
        if extra > remaining_gas:
            new_failures[account_id] = new_failures[account_id] + 1 if account_id in new_failures else 1
        if extra > remaining_gas + tgas_100:
            new_failures_with_extra_100tgas[account_id] = new_failures_with_extra_100tgas[account_id] + 1 if account_id in new_failures_with_extra_100tgas else 1
        if extra > remaining_gas + 2 * tgas_100:
            new_failures_with_extra_200tgas[account_id] = new_failures_with_extra_200tgas[account_id] + 1 if account_id in new_failures_with_extra_200tgas else 1

total_fn_calls = ok + fail

print("new cost: ", str(int(sys.argv[2]) * 10) + "Ggas")
print("total blocks:", total_blocks)
print("total function calls:", total_fn_calls)
print("current failures:", str(round(fail / total_fn_calls * 100, 2)) + "%")
print("additional new failures (300Tgas):", fail_rate(total_count(new_failures), total_fn_calls), get_top_10(new_failures))
print("additional new failures (400Tgas):", fail_rate(total_count(new_failures_with_extra_100tgas), total_fn_calls), get_top_10(new_failures_with_extra_100tgas))
print("additional new failures (500Tgas):", fail_rate(total_count(new_failures_with_extra_200tgas), total_fn_calls), get_top_10(new_failures_with_extra_200tgas))
print('')
