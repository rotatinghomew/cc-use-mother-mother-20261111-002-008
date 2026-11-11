测试：python3 -m unittest discover -s tests/privacy_batches -p 'test_*.py'
构建：python3 -m compileall -q registry_privacy_gateway
验收：python3 -m registry_privacy_gateway.gateway.transfer fixtures/demo.json --db /tmp/registry_privacy_gateway-acceptance.sqlite3
