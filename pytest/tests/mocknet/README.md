Mirror transactions from a given network into a custom mocktest network and add load

1. Setup a custom mocknet network following the README in the `provisioning/terraform/network/mocknet/mirror/` directory of the [near-ops](https://github.com/near/near-ops) repo.
- you'll need the `unique_id`, `chain_id`, and `start_height` from this setup when running the mirror.py test script in 2.
2. Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} setup` replacing the `{}`s with appropriate values from the `nearcore/pytest` directory.
- This may take a bit to setup, so if you get a Broken pipe make sure to complete the following steps:
- Get the instances associated with your project: `gcloud --project near-mocknet compute instances list | grep {unique_id}`
- ssh into the instances and check `/home/ubuntu/.near/logs/amend-genesis.txt` on the nodes to make sure there's nothing bad in there
- then also run `du -sh /home/ubuntu/.near/records.json` on the validators and  `du -sh /home/ubuntu/.near/target/records.json` on the traffic generator. If it's 27 GB (and there's no neard process when you run `ps -C neard`) it should be done.
- Run `python3 tests/mocknet/mirror.py --chain-id {chain_id} --start-height {start_height} --unique-id {unique_id} make-backups` replacing the `{}`s with appropriate values. This step will take >12 hours to run (shouldn't be >24 hours).
3. Monitoring
- TODO