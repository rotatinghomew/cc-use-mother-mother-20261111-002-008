from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from registry_privacy_gateway.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from registry_privacy_gateway.envelopes.batch_contract import build_processor as batch_contract_factory
from registry_privacy_gateway.consent.purpose_policy import build_processor as purpose_policy_factory
from registry_privacy_gateway.pseudonyms.token_service import build_processor as token_service_factory
from registry_privacy_gateway.minimization.field_filter import build_processor as field_filter_factory
from registry_privacy_gateway.validation.record_rules import build_processor as record_rules_factory
from registry_privacy_gateway.quarantine.isolation import build_processor as isolation_factory
from registry_privacy_gateway.receipts.ack_protocol import build_processor as ack_protocol_factory
from registry_privacy_gateway.ledger.immutable_log import build_processor as immutable_log_factory

PROCESSOR_FACTORIES = [batch_contract_factory, purpose_policy_factory, token_service_factory, field_filter_factory, record_rules_factory, isolation_factory, ack_protocol_factory, immutable_log_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='registry_privacy_gateway')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'registry_privacy_gateway',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        purpose=record.get("attributes",{}).get("purpose","registry-monitoring")
        allowed=purpose in {"registry-monitoring","quality-audit","authorized-research"}
        record["privacy_envelope"]={"purpose":purpose,"allowed":allowed,"removed_fields":[],"ack_sequence":0}
        if not allowed: raise DomainError("purpose is not authorized")
        record["subject_id"]=fingerprint({"purpose":purpose,"subject":record.get("subject_id","")})[:24]
        return record

    def architecture_plan(self, ctx):
        return list(self.processors)

    def architecture_finalize(self, ctx):
        envelope=ctx.payload["privacy_envelope"]
        for field in ["direct_name","phone","address"]:
            if field in ctx.payload: envelope["removed_fields"].append(field); ctx.payload.pop(field,None)
        envelope["ack_sequence"]=ctx.revision
        envelope["receipt_digest"]=fingerprint({"case":ctx.case_id,"flags":ctx.flags,"revision":ctx.revision})
        ctx.emit("privacy_gateway","ack_issued",sequence=envelope["ack_sequence"])

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
