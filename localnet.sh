rm -rf /home/birchmd/tmp-near-localnet/
rm -rf /home/birchmd/.nearup/logs/localnet/*.log
mkdir /home/birchmd/tmp-near-localnet/
./target/release/neard --home /home/birchmd/tmp-near-localnet/ localnet
cp /home/birchmd/tmp-near-localnet/node0/validator_key.json /home/birchmd/.near-credentials/localnet/node0.json
cp /home/birchmd/tmp-near-localnet/node1/validator_key.json /home/birchmd/.near-credentials/localnet/node1.json
cp /home/birchmd/tmp-near-localnet/node2/validator_key.json /home/birchmd/.near-credentials/localnet/node2.json
cp /home/birchmd/tmp-near-localnet/node3/validator_key.json /home/birchmd/.near-credentials/localnet/node3.json
nearup run --home /home/birchmd/tmp-near-localnet/ --binary-path ./target/release/ localnet
