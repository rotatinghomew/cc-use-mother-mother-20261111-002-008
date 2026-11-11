from __future__ import annotations
import argparse
import json
from registry_privacy_gateway.service import run_fixture

def main() -> int:
    parser=argparse.ArgumentParser(description='肿瘤登记批次隐私交换网关')
    parser.add_argument("fixture")
    parser.add_argument("--db",default="/tmp/registry_privacy_gateway.sqlite3")
    args=parser.parse_args()
    result=run_fixture(args.fixture,args.db)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
