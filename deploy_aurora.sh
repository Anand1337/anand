NEAR_ENV=localnet /home/birchmd/upstream/near-cli/bin/near create-account --networkId localnet --keyPath /home/birchmd/tmp-near-localnet/node0/validator_key.json --masterAccount node0 --initialBalance 10000 aurora.node0
NEAR_ENV=localnet /home/birchmd/upstream/near-cli/bin/near create-account --networkId localnet --keyPath /home/birchmd/tmp-near-localnet/node0/validator_key.json --masterAccount node0 --initialBalance 10000 tester.node0

/home/birchmd/upstream/aurora-is-near/aurora-cli/lib/aurora.js --network localnet --signer aurora.node0 --engine aurora.node0 install --owner aurora.node0 /home/birchmd/aurora-engine/mainnet-release.wasm

/home/birchmd/upstream/aurora-is-near/aurora-cli/lib/aurora.js --network localnet --signer tester.node0 --engine aurora.node0 deploy-code $(cat contract.hex)
NEAR_ENV=localnet /home/birchmd/upstream/near-cli/bin/near state --networkId localnet --keyPath /home/birchmd/tmp-near-localnet/node0/validator_key.json --masterAccount node0 tester.node0
/home/birchmd/upstream/aurora-is-near/aurora-cli/lib/aurora.js --network localnet --signer tester.node0 --engine aurora.node0 call 0x527a5c6655b4a196d1e05dbdc8810fe34a924411 0x558badd700000000000000000000000000000000000000000000000000000000000001f4
NEAR_ENV=localnet /home/birchmd/upstream/near-cli/bin/near state --networkId localnet --keyPath /home/birchmd/tmp-near-localnet/node0/validator_key.json --masterAccount node0 tester.node0
