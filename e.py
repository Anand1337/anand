import sys

ok, fail = 0, 0
current_cost = 16101955926
new_cost =    140000000000
tgas_3 =     3000000000000
tgas_100 = 100000000000000
new_failures, new_failures_with_extra_100tgas, new_failures_with_extra_200tgas = 0, 0, 0
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

        extra = real_reads * (new_cost - current_cost)
        if extra > remaining_gas:
            new_failures += 1
        if extra > remaining_gas + tgas_100:
            new_failures_with_extra_100tgas += 1
        if extra > remaining_gas + 2 * tgas_100:
            new_failures_with_extra_200tgas += 1


print("total blocks:", total_blocks)
print("total function calls:", ok + fail)
print("current failures:", str(round(fail / (ok + fail) * 100, 2)) + "%")
print("additional new failures (300Tgas):", str(round(new_failures / (ok + fail) * 100, 2)) + "%")
print("additional new failures (400Tgas):", str(round(new_failures_with_extra_100tgas / (ok + fail) * 100, 2)) + "%")
print("additional new failures (500Tgas):", str(round(new_failures_with_extra_200tgas / (ok + fail) * 100, 2)) + "%")
