ok, fail = 0, 0
current_cost = 16101955926
new_cost = 140000000000
new_failures = 0
with open("fn_data") as f:
    for line in f:
        if not line.startswith("fn_fail") and not line.startswith("fn_ok"):
            continue

        tokens = line.rstrip().split(' ')
        if tokens[0] == "fn_fail":
            fail += 1
            continue
        ok += 1

        real_reads = int(tokens[1])
        remaining_gas = int(tokens[4]) - int(tokens[3])

        extra = real_reads * (new_cost - current_cost)
        if extra > remaining_gas:
            new_failures += 1


print("overall fn-s:", ok + fail)
print("current failure rate:", str(round(fail / (ok + fail) * 100, 2)) + "%")
print("additional new failures:", str(round(new_failures / (ok + fail) * 100, 2)) + "%")
