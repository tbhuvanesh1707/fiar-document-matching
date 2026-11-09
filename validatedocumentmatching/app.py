"""FIAR app services: extraction review, accounting targets and evidence ZIPs."""


# domain.py — consolidated
"""Review rules kept independent of Streamlit and Databricks for testing."""
from urllib.parse import quote, urlparse


class ReviewError(ValueError):
    pass


def validate_decision(row, expected_hash, action, comment, reviewer_id, email):
    if action not in {"APPROVE", "REJECT"}:
        raise ReviewError("Choose Accept match or Reject match.")
    if not reviewer_id or not email:
        raise ReviewError("Your signed-in reviewer identity is unavailable. Contact the app owner.")
    if not row or row.get("snapshot_hash") != expected_hash:
        raise ReviewError("This match changed since you opened it. Refresh and review the latest data.")
    if not row.get("target_record_id") or row.get("extraction_error"):
        raise ReviewError("This item has no valid candidate to review.")
    if action == "REJECT" and not comment.strip():
        raise ReviewError("Please provide a reason for rejecting this match.")
    if action == "APPROVE" and row.get("amount_match") != "MATCH" and not comment.strip():
        raise ReviewError("Explain why you accept a match with a missing or different amount.")
    if len(comment) > 4000:
        raise ReviewError("Keep comments within 4,000 characters.")


def document_link(host, uri):
    """Construct a same-workspace link; never use a stored arbitrary URL."""
    path = (uri or "").removeprefix("dbfs:")
    hostname = urlparse(host if "://" in host else "https://" + host).netloc
    if not hostname or not path.startswith("/Volumes/"):
        return None
    return f"https://{hostname}/ajax-api/2.0/fs/files{quote(path, safe='/')}"


def aggregate_votes(events, candidate_key, snapshot_hash):
    latest = {}
    for event in events:
        if (event["candidate_key"], event["snapshot_hash"]) != (candidate_key, snapshot_hash):
            continue
        previous = latest.get(event["reviewer_id"])
        if previous is None or (event["reviewed_at"], event["decision_id"]) > (
                previous["reviewed_at"], previous["decision_id"]):
            latest[event["reviewer_id"]] = event
    approvals = sum(e["decision"] == "APPROVE" for e in latest.values())
    rejections = sum(e["decision"] == "REJECT" for e in latest.values())
    status = "CONFLICT" if approvals and rejections else (
        "APPROVED" if approvals else "REJECTED" if rejections else "PENDING")
    return status, approvals, rejections


# support.py — consolidated
"""Stable candidate identity and target accounting records."""
import hashlib
import json

SNAPSHOT_FIELDS = ['source_row_key', 'document_id', 'document_row_id', 'document_type',
    'storage_uri', 'file_name', 'invoice_number', 'dssn', 'voucher_number',
    'reference_number', 'contract_number', 'doc_amount', 'target_schema',
    'target_table', 'target_record_id', 'system_amount', 'amount_match',
    'actual_record_json', 'source_excerpt', 'extraction_error']


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def normalize(row):
    row = dict(row)
    row['candidate_key'] = digest([row.get(k) for k in ['source_row_key', 'target_schema', 'target_table', 'target_record_id']])
    row['snapshot_hash'] = digest({k: row.get(k) for k in SNAPSHOT_FIELDS})
    status = str(row.get('review_status') or 'PROPOSED').upper()
    row['decision_status'] = 'PENDING' if status not in {'APPROVED', 'REJECTED', 'CONFLICT', 'UNMATCHED', 'EXTRACTION_ERROR'} else status
    row['approvals'] = 1 if status == 'APPROVED' else 0
    row['rejections'] = 1 if status == 'REJECTED' else 0
    return row


def group_targets(rows):
    grouped = {}
    for row in rows:
        if row.get('target_record_id') is not None and str(row['target_record_id']) != '':
            identity = tuple(str(row.get(k) or '') for k in ['target_schema', 'target_table', 'target_record_id'])
            grouped.setdefault(identity, []).append(row)
    targets = []
    for (schema, table, record), candidates in sorted(grouped.items()):
        targets.append(dict(target_schema=schema, target_table=table, target_record_id=record,
            accounting_snapshots=sorted({r['actual_record_json'] for r in candidates if r.get('actual_record_json')}),
            document_count=len({r.get('document_id') for r in candidates}),
            approved_documents=len({r.get('document_id') for r in candidates if r['decision_status'] == 'APPROVED'}),
            pending_documents=len({r.get('document_id') for r in candidates if r['decision_status'] == 'PENDING'}),
            conflict_rows=sum(r['decision_status'] == 'CONFLICT' for r in candidates)))
    return targets


# bundles.py — consolidated
"""Build bounded evidence ZIPs from saved candidate rows and original volume files."""
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import PurePosixPath
import re
from zipfile import ZipFile, ZIP_DEFLATED


MAX_DOCUMENTS = 100
MAX_CANDIDATES = 1000
MAX_BYTES = 50 * 1024 * 1024
KEY_FIELDS = ('invoice_id', 'disbursement_id', 'invoice_number', 'dssn',
              'voucher_number', 'treasury_schedule_no', 'reference_number',
              'contract_number', 'vendor_id', 'vendor_code', 'vendor_name',
              'vendor', 'total_amount', 'amount', 'invoice_date', 'posting_date', 'id')


def key_values(raw):
    """Use accounting fields only; never substitute document-extracted values."""
    try:
        record = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return {'Record': 'Invalid accounting JSON'}
    if not isinstance(record, dict):
        return {'Record': 'Accounting snapshot unavailable'}
    keys = [k for k in KEY_FIELDS if k in record]
    if not keys:
        keys = sorted(record)[:8]
    return {k: record[k] for k in keys}


def safe_name(value):
    return re.sub(r'[^A-Za-z0-9._-]', '_', str(value or 'record'))[:100].strip('.') or 'record'


def volume_path(uri, allowed_prefix):
    path = (uri or '').removeprefix('dbfs:')
    prefix = allowed_prefix.rstrip('/')
    parts = path.split('/')
    if (not prefix.startswith('/Volumes/') or len(prefix.split('/')) < 5
            or any(p in {'.', '..', ''} for p in parts[1:])
            or any(c in path for c in ['\\', '\x00', '\r', '\n'])
            or '%' in path or not path.startswith(prefix + '/')
            or len(parts) < 6):
        raise ReviewError('An evidence file is outside the configured document volume or has an invalid path.')
    return path


def bundle_signature(rows, include_pending):
    return sha256(json.dumps([bool(include_pending), sorted(
        (r['candidate_key'], r['snapshot_hash'], r['decision_status'], r.get('storage_uri'))
        for r in rows)], default=str).encode()).hexdigest()


def build_bundle(rows, read_file, allowed_prefix, include_pending=False,
                 max_documents=MAX_DOCUMENTS, max_bytes=MAX_BYTES):
    allowed = {'APPROVED', 'PENDING'} if include_pending else {'APPROVED'}
    eligible = [r for r in rows if r.get('decision_status') in allowed and not r.get('extraction_error')]
    if not eligible:
        raise ReviewError('No eligible document links exist for this target and bundle option.')
    if len(eligible) > MAX_CANDIDATES:
        raise ReviewError('Too many supporting rows for one bundle. Ask the app owner for a bulk export.')
    targets = {(r.get('target_schema'), r.get('target_table'), r.get('target_record_id')) for r in eligible}
    if len(targets) != 1 or not next(iter(targets))[2]:
        raise ReviewError('A bundle must contain exactly one accounting target record.')
    target = next(iter(targets))
    documents = {}
    for row in eligible:
        path = volume_path(row.get('storage_uri'), allowed_prefix)
        documents.setdefault(path, []).append(row)
    if len(documents) > max_documents:
        raise ReviewError(f'This bundle exceeds the {max_documents}-document limit.')
    records = sorted({r.get('actual_record_json') or '{}' for r in eligible})
    try:
        snapshots = [json.loads(r) for r in records]
    except (ValueError, TypeError) as error:
        raise ReviewError('The accounting snapshot is invalid. Refresh the saved review data.') from error
    manifest = dict(created_at=datetime.now(timezone.utc).isoformat(),
                    target_schema=target[0], target_table=target[1], target_record_id=target[2],
                    includes_pending=include_pending,
                    evidence_note='Files are current volume bytes at export time. SHA-256 hashes are of exported files.',
                    accounting_snapshot_count=len(snapshots), documents=[])
    output = BytesIO()
    total_bytes = 0
    with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
        for index, (path, links) in enumerate(sorted(documents.items()), 1):
            data = read_file(path, max_bytes - total_bytes)
            if not isinstance(data, bytes):
                raise ReviewError('The source document download did not return bytes.')
            total_bytes += len(data)
            if total_bytes > max_bytes:
                raise ReviewError('Evidence bundle exceeds its byte limit. No partial ZIP was produced.')
            archive_name = f'documents/{index:03d}_{safe_name(PurePosixPath(path).name)}'
            archive.writestr(archive_name, data)
            manifest['documents'].append(dict(
                storage_uri=path, archive_name=archive_name, size_bytes=len(data),
                sha256=sha256(data).hexdigest(), supporting_rows=[{
                    k: r.get(k) for k in ['document_id', 'document_row_id', 'document_type',
                    'candidate_key', 'snapshot_hash', 'decision_status', 'amount_match',
                    'approvals', 'rejections', 'last_reviewed_at']} for r in links]))
        metadata = {
            'manifest.json': manifest,
            'accounting_records.json': snapshots,
            'matching_evidence.json': [{k: r.get(k) for k in [
                'source_row_key', 'document_id', 'document_row_id', 'invoice_number', 'dssn',
                'voucher_number', 'reference_number', 'contract_number', 'doc_amount',
                'system_amount', 'amount_match', 'decision_status', 'snapshot_hash']} for r in eligible]
        }
        for name, value in metadata.items():
            encoded = json.dumps(value, ensure_ascii=False, indent=2, default=str).encode('utf-8')
            total_bytes += len(encoded)
            if total_bytes > max_bytes:
                raise ReviewError('Evidence bundle exceeds its byte limit. No partial ZIP was produced.')
            archive.writestr(name, encoded)
    prefix = 'review' if include_pending else 'approved'
    return output.getvalue(), f'fiar_{prefix}_{safe_name(target[1])}_{safe_name(target[2])}.zip'



# Human labels and audit assessments are append-only events.
import os
import time
import logging
import uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

DOC_TYPES = ['invoice', 'voucher', 'sot_detail', 'gwa_detail', 'contract_pay_detail',
             'contract', 'modification', 'receiving_report', 'disbursement_authorization', 'other']
DOCUMENT_FIELDS = ['invoice_number', 'dssn', 'voucher_number', 'reference_number',
                   'contract_number', 'doc_amount', 'vendor_name', 'document_date']
REASONS = ['IDENTIFIERS_VERIFIED', 'AMOUNT_VERIFIED', 'WRONG_VENDOR', 'WRONG_IDENTIFIER',
           'WRONG_AMOUNT', 'WRONG_PERIOD', 'DUPLICATE_DOCUMENT', 'INSUFFICIENT_EVIDENCE', 'OTHER']
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
LABEL_VERSION = 'fiar-human-label-v1'
SNAPSHOT_FIELDS = list(dict.fromkeys(SNAPSHOT_FIELDS + ['vendor_name','document_date','document_content_hash','feature_origin']))


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(',', ':'))


def record_hash(raw):
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        raise ReviewError('The accounting snapshot is invalid.')
    if not isinstance(value, dict):
        raise ReviewError('The accounting snapshot must contain a record object.')
    return digest(value)


def target_identity(row):
    return tuple(str(row.get(k) or '') for k in ['target_schema', 'target_table', 'target_record_id'])


def transaction_key(row):
    return digest(target_identity(row))


def latest_events(events, keys):
    latest = {}
    for event in events:
        key = tuple(str(event.get(k) or '') for k in keys)
        order = (str(event.get('reviewed_at') or ''), str(event.get('event_id') or ''))
        previous = latest.get(key)
        if previous is None or order > (str(previous.get('reviewed_at') or ''), str(previous.get('event_id') or '')):
            latest[key] = event
    return list(latest.values())


def apply_match_labels(source_rows, events):
    """Apply only labels for the exact source/accounting snapshot shown to a reviewer."""
    rows = {}
    for source in source_rows:
        row = normalize(source)
        if row['candidate_key'] in rows:
            raise ReviewError('Duplicate candidate identities exist in the saved review table. Deduplicate them before labeling.')
        rows[row['candidate_key']] = row
    manual_versions = latest_events([e for e in events if e.get('label_origin') in {'MANUAL', 'UPLOAD'}], ['candidate_key'])
    for event in manual_versions:
        if event['candidate_key'] not in rows:
            row = normalize(json.loads(event['source_snapshot_json']))
            rows[row['candidate_key']] = row
    votes = latest_events(events, ['candidate_key', 'snapshot_hash', 'reviewer_email'])
    for row in rows.values():
        current = [e for e in votes if (e['candidate_key'], e['snapshot_hash']) == (row['candidate_key'], row['snapshot_hash'])]
        if current:
            positive = sum(e['label'] == 'TRUE_MATCH' for e in current)
            negative = sum(e['label'] == 'NON_MATCH' for e in current)
            row.update(approvals=positive, rejections=negative,
                decision_status='CONFLICT' if positive and negative else 'APPROVED' if positive else 'REJECTED',
                label_event_ids=sorted(e['event_id'] for e in current),
                last_reviewed_at=max(str(e['reviewed_at']) for e in current),
                label_origin=current[-1].get('label_origin'), review_origin='HUMAN_LABEL')
            row['review_status'] = row['decision_status']
    # Accepting two different targets for the same source row needs resolution.
    accepted = {}
    for row in rows.values():
        if row['decision_status'] == 'APPROVED':
            accepted.setdefault((str(row['document_id']),str(row.get('document_row_id') or '0')), set()).add(target_identity(row))
    for row in rows.values():
        if row['decision_status'] == 'APPROVED' and len(accepted.get((str(row['document_id']),str(row.get('document_row_id') or '0')), set())) > 1:
            row['decision_status'] = row['review_status'] = 'CONFLICT'
    return list(rows.values())


def resolution_state(transaction, candidates):
    current, stale = [], 0
    for row in candidates:
        if row.get('actual_record_json') and record_hash(row['actual_record_json']) == record_hash(transaction['actual_record_json']):
            current.append(row)
        else:
            stale += 1
    statuses = [r['decision_status'] for r in current]
    if 'CONFLICT' in statuses:
        return 'CONFLICT'
    if 'APPROVED' in statuses:
        return 'HAS_ACCEPTED_EVIDENCE'
    if current and all(s == 'REJECTED' for s in statuses):
        return 'ALL_REJECTED'
    if current:
        return 'CANDIDATES_TO_REVIEW'
    return 'STALE_CANDIDATES' if stale else 'NO_CANDIDATES'


def evidence_context(transaction, candidates):
    evidence = [{k: r.get(k) for k in ['candidate_key', 'snapshot_hash', 'document_id',
        'document_row_id', 'storage_uri', 'decision_status', 'label_event_ids']}
        for r in candidates if r.get('actual_record_json') and
        record_hash(r['actual_record_json']) == record_hash(transaction['actual_record_json'])]
    evidence.sort(key=lambda r: (r['candidate_key'], r['snapshot_hash']))
    return evidence, digest([record_hash(transaction['actual_record_json']), evidence])


def validate_upload(filename, data):
    name = safe_name(PurePosixPath(filename.replace('\\', '/')).name)
    suffix = PurePosixPath(name).suffix.lower()
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise ReviewError('Choose a non-empty file no larger than 20 MiB.')
    if suffix not in {'.pdf', '.png', '.jpg', '.jpeg', '.md', '.txt'}:
        raise ReviewError('Upload a PDF, PNG, JPG, Markdown or text document.')
    if suffix == '.pdf' and not data.startswith(b'%PDF-'):
        raise ReviewError('The selected file does not have a PDF header.')
    if suffix in {'.png', '.jpg', '.jpeg'}:
        from PIL import Image
        try:
            with Image.open(BytesIO(data)) as picture:
                if picture.format not in ({'PNG'} if suffix == '.png' else {'JPEG'}):
                    raise ValueError('Image format does not match extension')
                picture.verify()
        except Exception as error:
            raise ReviewError('The selected image could not be verified.') from error
    if suffix in {'.md', '.txt'}:
        try:
            text = data.decode('utf-8')
            if '\x00' in text:
                raise ValueError('Binary text')
        except (UnicodeDecodeError, ValueError) as error:
            raise ReviewError('Text documents must contain UTF-8 text.') from error
    return name, sha256(data).hexdigest()


def feature_snapshot(row):
    # Human labels, review status, auditor results and notes are never input features.
    return dict(schema_version=LABEL_VERSION,
        document={k: row.get(k) for k in ['document_type', 'document_id', 'document_row_id',
            'document_content_hash', 'source_excerpt'] + DOCUMENT_FIELDS},
        transaction=json.loads(row['actual_record_json']),
        comparison={'amount_match': row.get('amount_match')},
        model={'version': row.get('model_version'), 'score': row.get('confidence_score')})


def qualified(name):
    parts = name.split('.')
    if len(parts) != 3 or any(not p or '`' in p for p in parts):
        raise ValueError('Use a catalog.schema.object name without backticks.')
    return '.'.join(f'`{p}`' for p in parts)


class ReviewStore:
    def __init__(self):
        self.client = WorkspaceClient()
        self.host = self.client.config.host
        self.warehouse = os.getenv('DATABRICKS_WAREHOUSE_ID', 'c8fb32ff1f1d2331')
        self.source = qualified(os.getenv('FIAR_SOURCE_TABLE', 'workspace.default.fiar_ai_link_review'))
        self.decisions = qualified(os.getenv('FIAR_DECISIONS_TABLE', 'workspace.default.fiar_review_decisions'))
        self.namespace = os.getenv('FIAR_LABEL_SCHEMA', 'workspace.default')
        if len(self.namespace.split('.')) != 2:
            raise ValueError('FIAR_LABEL_SCHEMA must be catalog.schema')
        self.document_prefix = os.getenv('FIAR_DOCUMENT_PREFIX', '/Volumes/workspace/default/fiar-docs').rstrip('/')
        self.invoice_table = qualified(os.getenv('FIAR_INVOICE_TABLE', 'workspace.default.payables_vendor_invoices'))
        self.disbursement_table = qualified(os.getenv('FIAR_DISBURSEMENT_TABLE', 'workspace.default.genledger_disbursements'))
        for key in ['match_labels', 'evidence_uploads', 'current_match_labels', 'accounting_transactions']:
            setattr(self, key, qualified(self.namespace + '.fiar_' + key))

        self.audit_assessments_table = self.object_name('audit_assessments')

    def object_name(self, suffix):
        return qualified(self.namespace + '.fiar_' + suffix)
    def query(self, statement, parameters=None):
        params = [StatementParameterListItem(name=k, value=str(v) if v is not None else None, type='STRING')
                  for k, v in (parameters or {}).items()]
        api = self.client.statement_execution
        response = api.execute_statement(warehouse_id=self.warehouse, statement=statement,
                                         parameters=params, wait_timeout='10s')
        deadline = time.monotonic() + 120
        while response.status and str(getattr(response.status.state, 'value', response.status.state)) in {'PENDING', 'RUNNING'}:
            if time.monotonic() > deadline:
                api.cancel_execution(response.statement_id)
                raise ReviewError('The warehouse query timed out. Retry after the warehouse is ready.')
            time.sleep(0.5)
            response = api.get_statement(response.statement_id)
        status = getattr(response.status.state, 'value', response.status.state) if response.status else None
        if status != 'SUCCEEDED':
            error = getattr(response.status, 'error', None)
            code = getattr(error, 'error_code', None) or status or 'UNKNOWN'
            code = getattr(code, 'value', code)
            message = getattr(error, 'message', None) or 'Databricks returned no detailed error message.'
            statement_id = getattr(response, 'statement_id', None) or 'unavailable'
            logging.error('Databricks SQL failed. statement_id=%s state=%s code=%s message=%s',
                          statement_id, status, code, message)
            hint = ''
            if any(token in message.upper() for token in ['UNRESOLVED_COLUMN', 'DELTA_MERGE_UNRESOLVED_EXPRESSION', 'TABLE_OR_VIEW_NOT_FOUND']):
                hint = ' Check that FIAR_SOURCE_TABLE and FIAR_DECISIONS_TABLE point to your existing tables and expected columns.'
            elif any(token in message.upper() for token in ['INSUFFICIENT_PERMISSIONS', 'PERMISSION_DENIED', 'NOT AUTHORIZED']):
                hint = ' Grant the app service principal the required table privileges.'
            raise ReviewError(f'Databricks SQL error [{code}]: {message[:1500]}{hint} Statement ID: {statement_id}')
        if not response.manifest or not response.manifest.schema:
            return []
        if response.manifest.truncated:
            raise ReviewError('The SQL result was truncated. Filter the source data before using this app.')
        columns = [c.name for c in response.manifest.schema.columns]
        chunk, rows = response.result, []
        while chunk:
            rows.extend(dict(zip(columns, row)) for row in (chunk.data_array or []))
            if chunk.next_chunk_index is None:
                break
            chunk = api.get_statement_result_chunk_n(response.statement_id, chunk.next_chunk_index)
        return rows

    def source_rows(self):
        rows = self.query(f'SELECT * FROM {self.source} ORDER BY document_id LIMIT 10001')
        if len(rows) > 10000:
            raise ReviewError('More than 10,000 source candidates; configure a filtered source before review.')
        return rows

    def label_events(self):
        rows = self.query(f'SELECT * FROM {self.current_match_labels} LIMIT 25001')
        if len(rows) > 25000:
            raise ReviewError('More than 25,000 current labels; add server-side paging before reviewing this dataset.')
        return rows

    def load_review_data(self):
        return apply_match_labels(self.source_rows(), self.label_events())

    def load_history(self):
        return self.query(f'SELECT * FROM {self.decisions} ORDER BY reviewed_at DESC LIMIT 1000')

    def training_history(self):
        return self.query(f'SELECT * FROM {self.match_labels} ORDER BY reviewed_at DESC LIMIT 1000')

    def current(self, row):
        candidates = [r for r in self.load_review_data() if r['candidate_key'] == row['candidate_key']]
        if len(candidates) != 1 or candidates[0]['snapshot_hash'] != row['snapshot_hash']:
            raise ReviewError('This document/accounting candidate changed. Refresh before labeling.')
        return candidates[0]

    def transaction(self, target):
        params = {k: target[k] for k in ['target_schema', 'target_table', 'target_record_id']}
        where = ' AND '.join(f'{k} = :{k}' for k in params)
        rows = self.query(f'SELECT * FROM {self.accounting_transactions} WHERE {where} LIMIT 2', params)
        if len(rows) != 1:
            raise ReviewError('The accounting transaction is missing or has duplicate IDs.')
        row = rows[0]
        row['transaction_key'] = transaction_key(row)
        return row

    def list_transactions(self, search='', offset=0, limit=20, unresolved_only=True):
        params = {'search': search.strip().lower()}
        rows = self.query(f'''SELECT * FROM {self.accounting_transactions}
            WHERE :search = '' OR contains(lower(concat_ws(' ', target_table, target_record_id, actual_record_json)), :search)
            ORDER BY target_table, target_record_id LIMIT 10001''', params)
        if len(rows) > 10000:
            raise ReviewError('More than 10,000 transactions match. Narrow the search.')
        candidates = self.load_review_data()
        for row in rows:
            row['transaction_key'] = transaction_key(row)
            linked = [c for c in candidates if target_identity(c) == target_identity(row)]
            row['resolution_status'] = resolution_state(row, linked)
            row['candidate_count'] = len(linked)
        if unresolved_only:
            rows = [r for r in rows if r['resolution_status'] != 'HAS_ACCEPTED_EVIDENCE']
        priority={'NO_CANDIDATES':0,'ALL_REJECTED':1,'STALE_CANDIDATES':2,'CONFLICT':3,'CANDIDATES_TO_REVIEW':4,'HAS_ACCEPTED_EVIDENCE':5}
        rows.sort(key=lambda r:(priority[r['resolution_status']],target_identity(r)))
        return rows[offset:offset+limit], len(rows)

    def target_candidates(self, target):
        rows = [r for r in self.load_review_data() if target_identity(r) == target_identity(target)]
        tx = self.transaction(target)
        for row in rows:
            if record_hash(row['actual_record_json']) != record_hash(tx['actual_record_json']):
                row['decision_status'] = 'STALE_ACCOUNTING'
        return rows

    def list_targets(self, search='', offset=0, limit=10):
        targets = group_targets(self.load_review_data())
        if search:
            targets = [r for r in targets if search.lower() in json_text(r).lower()]
        total=len(targets)
        page=targets[offset:offset+limit]
        candidates=self.load_review_data()
        for target in page:
            tx=self.transaction(target)
            current=[r for r in candidates if target_identity(r)==target_identity(target) and record_hash(r['actual_record_json'])==record_hash(tx['actual_record_json'])]
            target['approved_documents']=len({r['document_id'] for r in current if r['decision_status']=='APPROVED'})
            target['pending_documents']=len({r['document_id'] for r in current if r['decision_status']=='PENDING'})
        return page,total

    def append_event(self, table, event):
        if not event.get('event_id'):
            raise ReviewError('An event ID is required for safe retries.')
        names = list(event)
        projection = ', '.join(f':{k} AS {k}' for k in names)
        self.query(f'''INSERT INTO {table} ({', '.join(names)}, reviewed_at)
            SELECT {projection}, current_timestamp()
            WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE event_id = :event_id)''', event)
        saved = self.query(f'SELECT * FROM {table} WHERE event_id = :event_id LIMIT 1', {'event_id': event['event_id']})
        if not saved:
            raise ReviewError('The event could not be confirmed. Retry the same submission.')
        for key in event:
            if key in event and str(saved[0].get(key)) != str(event[key]):
                raise ReviewError('This submission ID is already associated with different data. Start a new submission.')
        return saved[0]

    def label_candidate(self, row, label, name, email, notes, event_id, reason='IDENTIFIERS_VERIFIED', origin='PROPOSED', fresh_check=True):
        if label not in {'TRUE_MATCH', 'NON_MATCH'} or reason not in REASONS:
            raise ReviewError('Choose a valid match label and reason.')
        if not name or not email:
            raise ReviewError('Open the app through Databricks for your authenticated reviewer identity.')
        if origin not in {'PROPOSED', 'MANUAL', 'UPLOAD'}:
            raise ReviewError('Invalid label origin.')
        row = self.current(row) if fresh_check else normalize(row)
        if not row.get('target_record_id') or row.get('extraction_error'):
            raise ReviewError('Choose a valid accounting target before recording a match/non-match.')
        if len(notes) > 4000 or ((label == 'NON_MATCH' or row.get('amount_match') != 'MATCH') and not notes.strip()):
            raise ReviewError('Provide a comment for a non-match or an amount exception (maximum 4,000 characters).')
        tx = self.transaction(row)
        if record_hash(tx['actual_record_json']) != record_hash(row['actual_record_json']):
            raise ReviewError('The accounting record changed. Refresh before labeling this candidate.')
        source_snapshot = {k: row.get(k) for k in SNAPSHOT_FIELDS + ['document_content_hash', 'model_version', 'confidence_score', 'feature_origin']}
        event = dict(event_id=event_id, candidate_key=row['candidate_key'], snapshot_hash=row['snapshot_hash'],
            transaction_key=transaction_key(tx), transaction_snapshot_hash=record_hash(tx['actual_record_json']),
            target_schema=row['target_schema'], target_table=row['target_table'], target_record_id=str(row['target_record_id']),
            document_id=str(row['document_id']), document_row_id=str(row.get('document_row_id') or '0'),
            source_row_key=row['source_row_key'], document_type=row['document_type'], storage_uri=row['storage_uri'],
            document_content_hash=row.get('document_content_hash'), label=label, label_origin=origin, reason_code=reason,
            reviewer_name=name, reviewer_email=email, notes=notes.strip(), label_schema_version=LABEL_VERSION,
            model_version=str(row.get('model_version') or 'UNRECORDED'),
            source_snapshot_json=json_text(source_snapshot), transaction_snapshot_json=tx['actual_record_json'],
            feature_snapshot_json=json_text(feature_snapshot(row)))
        return self.append_event(self.match_labels, event)

    def save_decision(self, record, decision, name, email, comments, decision_id):
        if decision not in {'APPROVED', 'REJECTED'}:
            raise ReviewError('Choose Approve or Reject.')
        result = self.label_candidate(record, 'TRUE_MATCH' if decision == 'APPROVED' else 'NON_MATCH',
            name, email, comments, decision_id, 'IDENTIFIERS_VERIFIED' if decision == 'APPROVED' else 'OTHER',
            record.get('label_origin') or 'PROPOSED')
        # Keep the old history table compatible. The durable training label is
        # authoritative even if this separate legacy-history copy cannot complete.
        try:
            self.mirror_legacy(record, decision, name, email, comments, decision_id)
        except Exception:
            logging.exception('Training label saved; legacy history copy failed for event %s', decision_id)
            return 'Training label saved. The legacy history copy failed; use Training labels to verify the saved event.'
        return 'Decision and training label saved.'

    def mirror_legacy(self, row, decision, name, email, notes, event_id):
        legacy = ['source_row_key', 'document_id', 'document_type', 'file_name', 'invoice_number',
                  'contract_number', 'doc_amount', 'system_amount', 'amount_match']
        params = {k: row.get(k) for k in legacy}
        params.update(decision_id=event_id, original_review_status=row.get('review_status'), decision=decision,
            reviewer_name=name, reviewer_email=email, review_comments=notes.strip())
        cols = list(params)
        self.query(f'''INSERT INTO {self.decisions} ({', '.join(cols)}, reviewed_at)
            SELECT {', '.join(':'+k for k in cols)}, current_timestamp()
            WHERE NOT EXISTS (SELECT 1 FROM {self.decisions} WHERE decision_id = :decision_id)''', params)

    def manual_match(self, document, tx, row_id, fields, name, email, notes, event_id, origin='MANUAL'):
        fresh = self.transaction(tx)
        if record_hash(fresh['actual_record_json']) != record_hash(tx['actual_record_json']):
            raise ReviewError('The selected transaction changed. Refresh it.')
        if not notes.strip():
            raise ReviewError('Explain why this document supports the transaction.')
        if not row_id.strip() or len(row_id) > 100:
            raise ReviewError('Provide a document row/page identifier (maximum 100 characters).')
        volume_path(document.get('storage_uri'), self.document_prefix)
        # Re-fetch registered evidence instead of accepting an arbitrary path.
        registered = self.find_document(document)
        record = dict(registered)
        record.update({k: fields.get(k) for k in DOCUMENT_FIELDS})
        if record.get('doc_amount'):
            from decimal import Decimal, InvalidOperation
            try:
                if not Decimal(str(record['doc_amount'])).is_finite():
                    raise InvalidOperation()
            except InvalidOperation as error:
                raise ReviewError('Enter a valid finite document amount without currency symbols.') from error
        record.update(source_row_key=f"manual:{registered['document_id']}:{row_id.strip()}", document_row_id=row_id.strip(),
            target_schema=fresh['target_schema'], target_table=fresh['target_table'], target_record_id=fresh['target_record_id'],
            actual_record_json=fresh['actual_record_json'], system_amount=fresh.get('system_amount'),
            review_status='PROPOSED', extraction_error=None, feature_origin='USER_ENTERED')
        from decimal import Decimal, InvalidOperation
        try:
            record['amount_match'] = 'MATCH' if Decimal(str(record['doc_amount'])) == Decimal(str(fresh['system_amount'])) else 'MISMATCH'
        except (InvalidOperation, KeyError, TypeError):
            record['amount_match'] = 'UNKNOWN'
        return self.label_candidate(normalize(record), 'TRUE_MATCH', name, email, notes, event_id,
                                    'IDENTIFIERS_VERIFIED', origin, fresh_check=False)

    def documents(self):
        docs = {}
        for row in self.source_rows():
            if row.get('storage_uri'):
                docs[str(row['document_id'])] = {k: row.get(k) for k in ['document_id','document_type','file_name',
                    'storage_uri','source_excerpt','document_content_hash']}
        uploads = self.query(f'SELECT * FROM {self.evidence_uploads} ORDER BY reviewed_at DESC LIMIT 10001')
        if len(uploads) > 10000:
            raise ReviewError('Too many registered uploads. Add document paging before continuing.')
        for upload in uploads:
            docs[upload['document_id']] = dict(upload, document_content_hash=upload['content_hash'])
        return list(docs.values())

    def find_document(self, document):
        found = [d for d in self.documents() if str(d['document_id']) == str(document['document_id']) and d['storage_uri'] == document['storage_uri']]
        if len(found) != 1:
            raise ReviewError('Choose a registered source document or upload a new one.')
        return found[0]

    def upload_document(self, filename, data, document_type, tx, name, email, upload_id):
        if document_type not in DOC_TYPES or not name or not email:
            raise ReviewError('Select a document type and use your signed-in identity.')
        uuid.UUID(upload_id)
        fname, content_hash = validate_upload(filename, data)
        fresh = self.transaction(tx)
        if record_hash(fresh['actual_record_json']) != record_hash(tx['actual_record_json']):
            raise ReviewError('The accounting record changed. Refresh before uploading evidence.')
        directory = volume_path(self.document_prefix + '/user_uploads/' + upload_id, self.document_prefix)
        path = directory + '/' + fname
        self.client.files.create_directory(directory_path=directory)
        try:
            self.client.files.upload(file_path=path, contents=BytesIO(data), overwrite=False)
        except Exception:
            # A prior upload may have completed before an API/SQL timeout.
            # Only reuse that path when its bytes match exactly.
            if sha256(self.read_document(path, MAX_UPLOAD_BYTES)).hexdigest() != content_hash:
                raise ReviewError('This upload ID already contains a different file. Start a new upload.')
        excerpt = data.decode('utf-8')[:1500] if PurePosixPath(fname).suffix.lower() in {'.md','.txt'} else None
        event = dict(event_id=upload_id, upload_id=upload_id, document_id='user-upload-'+upload_id,
            document_type=document_type, file_name=fname, storage_uri=path, content_hash=content_hash,
            size_bytes=str(len(data)), transaction_key=transaction_key(fresh), reviewer_name=name,
            reviewer_email=email, source_excerpt=excerpt, extraction_status='PENDING_EXTRACTION')
        saved = self.append_event(self.evidence_uploads, event)
        return dict(saved, document_content_hash=content_hash)

    def audit_assessments(self, tx, context_hash=None):
        rows = self.query(f'SELECT * FROM {self.audit_assessments_table} WHERE transaction_key = :key ORDER BY reviewed_at DESC LIMIT 1000',
                          {'key': transaction_key(tx)})
        return [r for r in rows if context_hash is None or r['audit_context_hash'] == context_hash]

    def save_audit(self, tx, candidates, outcome, criterion, criteria_version, issue, notes, name, email, event_id):
        if outcome not in {'PASS', 'FAIL', 'NEEDS_REVIEW', 'NOTES_ONLY'}:
            raise ReviewError('Choose a valid audit outcome.')
        if not name or not email or not criterion.strip() or not criteria_version.strip():
            raise ReviewError('Auditor identity, audit test and criteria version are required.')
        if len(notes) > 4000 or (outcome != 'PASS' and not notes.strip()):
            raise ReviewError('Provide auditor notes for this outcome (maximum 4,000 characters).')
        if outcome == 'FAIL' and not issue.strip():
            raise ReviewError('Provide an issue category for a failed audit test.')
        fresh = self.transaction(tx)
        if record_hash(fresh['actual_record_json']) != record_hash(tx['actual_record_json']):
            raise ReviewError('The accounting record changed. Review it again.')
        evidence, expected = evidence_context(tx, candidates)
        current, actual = evidence_context(fresh, self.target_candidates(fresh))
        if actual != expected:
            raise ReviewError('The supporting evidence changed. Review the updated evidence before assessing it.')
        features = dict(transaction=json.loads(fresh['actual_record_json']),
            evidence_count=len(current), accepted_document_count=len({r['document_id'] for r in current if r['decision_status']=='APPROVED'}),
            pending_count=sum(r['decision_status']=='PENDING' for r in current),
            rejected_count=sum(r['decision_status']=='REJECTED' for r in current))
        event = dict(event_id=event_id, transaction_key=transaction_key(fresh),
            target_schema=fresh['target_schema'], target_table=fresh['target_table'], target_record_id=str(fresh['target_record_id']),
            transaction_snapshot_hash=record_hash(fresh['actual_record_json']), audit_context_hash=expected,
            outcome=outcome, audit_test_code=criterion.strip(), criteria_version=criteria_version.strip(),
            issue_category=issue.strip(), notes=notes.strip(), reviewer_name=name, reviewer_email=email,
            transaction_snapshot_json=fresh['actual_record_json'], evidence_snapshot_json=json_text(evidence),
            feature_snapshot_json=json_text(features), label_schema_version=LABEL_VERSION)
        return self.append_event(self.audit_assessments_table, event)
    def read_document(self, path, remaining_bytes):
        path = volume_path(path, self.document_prefix)
        response = self.client.files.download(file_path=path)
        stream = response.contents
        if stream is None:
            raise ReviewError('No document content was returned.')
        try:
            if response.content_length is not None and response.content_length > remaining_bytes:
                raise ReviewError('The evidence bundle exceeds 50 MiB.')
            chunks, size = [], 0
            while chunk := stream.read(min(1024*1024, remaining_bytes-size+1)):
                size += len(chunk)
                if size > remaining_bytes:
                    raise ReviewError('The evidence bundle exceeds 50 MiB.')
                chunks.append(chunk)
            return b''.join(chunks)
        finally:
            stream.close()



MATCH_FIELDS = ['event_id','candidate_key','snapshot_hash','transaction_key','transaction_snapshot_hash',
    'target_schema','target_table','target_record_id','document_id','document_row_id','source_row_key','document_type',
    'storage_uri','document_content_hash','label','label_origin','reason_code','reviewer_name','reviewer_email','notes',
    'label_schema_version','model_version','source_snapshot_json','transaction_snapshot_json','feature_snapshot_json']
UPLOAD_FIELDS = ['event_id','upload_id','document_id','document_type','file_name','storage_uri','content_hash',
    'size_bytes','transaction_key','reviewer_name','reviewer_email','source_excerpt','extraction_status']
AUDIT_FIELDS = ['event_id','transaction_key','target_schema','target_table','target_record_id',
    'transaction_snapshot_hash','audit_context_hash','outcome','audit_test_code','criteria_version','issue_category',
    'notes','reviewer_name','reviewer_email','transaction_snapshot_json','evidence_snapshot_json','feature_snapshot_json','label_schema_version']


def workflow_setup_sql(store):
    """Owner-run, additive setup; existing source/decision/accounting tables are untouched."""
    ns = store.namespace
    q = lambda name: qualified(ns+'.fiar_'+name)
    statements = ['-- Run once as schema owner. New tables collect labels; existing tables are not altered.']
    for suffix, fields, checks in [
        ('match_labels', MATCH_FIELDS, "CONSTRAINT valid_match_label CHECK (label IN ('TRUE_MATCH','NON_MATCH')), CONSTRAINT valid_match_origin CHECK (label_origin IN ('PROPOSED','MANUAL','UPLOAD'))"),
        ('evidence_uploads', UPLOAD_FIELDS, ''),
        ('audit_assessments', AUDIT_FIELDS, "CONSTRAINT valid_audit_outcome CHECK (outcome IN ('PASS','FAIL','NEEDS_REVIEW','NOTES_ONLY'))")]:
        columns = ',\n  '.join(f'{f} STRING' + (' NOT NULL' if f in {'event_id','reviewer_email'} else '') for f in fields)
        statements.append(f"CREATE TABLE IF NOT EXISTS {q(suffix)} (\n  {columns},\n  reviewed_at TIMESTAMP NOT NULL" +
                          (',\n  '+checks if checks else '') + ") USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');")
    statements.append(f'''CREATE OR REPLACE VIEW {q('accounting_transactions')} AS
        SELECT 'payables' AS target_schema, 'vendor_invoices' AS target_table,
            cast(i.invoice_id AS STRING) AS target_record_id, cast(i.total_amount AS STRING) AS system_amount,
            to_json(struct(i.*)) AS actual_record_json FROM {store.invoice_table} i
        UNION ALL
        SELECT 'genledger' AS target_schema, 'disbursements' AS target_table,
            cast(d.disbursement_id AS STRING) AS target_record_id, cast(d.amount AS STRING) AS system_amount,
            to_json(struct(d.*)) AS actual_record_json FROM {store.disbursement_table} d;''')
    statements.append(f'''CREATE OR REPLACE VIEW {q('current_match_labels')} AS
        SELECT * FROM {q('match_labels')}
        QUALIFY row_number() OVER (PARTITION BY candidate_key, snapshot_hash, reviewer_email
            ORDER BY reviewed_at DESC, event_id DESC)=1;''')
    statements.append(f'''CREATE OR REPLACE VIEW {q('match_training_examples')} AS
        WITH consensus AS (SELECT candidate_key, snapshot_hash, document_type, document_id, document_row_id, transaction_key,
            transaction_snapshot_hash,
            coalesce(document_content_hash, document_id) AS document_group_key,
            max(feature_snapshot_json) AS feature_snapshot_json, max(model_version) AS model_version,
            max(label_schema_version) AS label_schema_version,
            max(label) AS label, CASE WHEN max(label)='TRUE_MATCH' THEN 1 ELSE 0 END AS label_value,
            collect_set(label_origin) AS label_origins, count(*) AS reviewer_count,
            max(reviewed_at) AS labeled_at
        FROM {q('current_match_labels')}
        GROUP BY candidate_key,snapshot_hash,document_type,document_id,document_row_id,transaction_key,
            transaction_snapshot_hash,coalesce(document_content_hash,document_id)
        HAVING count(DISTINCT label)=1), competing AS (
            SELECT document_id,document_row_id,document_group_key FROM consensus WHERE label='TRUE_MATCH'
            GROUP BY document_id,document_row_id,document_group_key HAVING count(DISTINCT transaction_key)>1
        )
        SELECT c.* FROM consensus c LEFT JOIN competing x
            ON c.document_id=x.document_id AND c.document_row_id=x.document_row_id AND c.document_group_key=x.document_group_key
        WHERE c.label='NON_MATCH' OR x.document_id IS NULL;''')
    statements.append(f'''CREATE OR REPLACE VIEW {q('current_audit_assessments')} AS
        SELECT * FROM {q('audit_assessments')}
        QUALIFY row_number() OVER (PARTITION BY transaction_key,audit_context_hash,audit_test_code,criteria_version,reviewer_email
            ORDER BY reviewed_at DESC,event_id DESC)=1;''')
    statements.append(f'''CREATE OR REPLACE VIEW {q('audit_training_examples')} AS
        SELECT transaction_key,transaction_snapshot_hash,audit_context_hash,audit_test_code,criteria_version,
            max(feature_snapshot_json) AS feature_snapshot_json,max(label_schema_version) AS label_schema_version,
            max(outcome) AS outcome,CASE WHEN max(outcome)='FAIL' THEN 1 ELSE 0 END AS potential_issue_label,
            count(*) AS reviewer_count,max(reviewed_at) AS labeled_at
        FROM {q('current_audit_assessments')}
        GROUP BY transaction_key,transaction_snapshot_hash,audit_context_hash,audit_test_code,criteria_version
        HAVING count(DISTINCT outcome)=1 AND max(outcome) IN ('PASS','FAIL');''')
    for doctype in ['invoice','voucher','sot_detail','gwa_detail','contract_pay_detail']:
        statements.append(f"CREATE OR REPLACE VIEW {q(doctype+'_match_training')} AS SELECT * FROM {q('match_training_examples')} WHERE document_type='{doctype}';")
    catalog, schema = ns.split('.')
    statements.append(f'''-- Replace APP_SERVICE_PRINCIPAL_ID with your existing app service principal ID.
-- GRANT USE CATALOG ON CATALOG `{catalog}` TO `APP_SERVICE_PRINCIPAL_ID`;
-- GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `APP_SERVICE_PRINCIPAL_ID`;
-- GRANT SELECT ON TABLE {store.source} TO `APP_SERVICE_PRINCIPAL_ID`;
-- GRANT SELECT, MODIFY ON TABLE {store.decisions} TO `APP_SERVICE_PRINCIPAL_ID`;''')
    for suffix in ['match_labels','evidence_uploads','audit_assessments']:
        statements.append(f'-- GRANT SELECT, MODIFY ON TABLE {q(suffix)} TO `APP_SERVICE_PRINCIPAL_ID`;')
    for suffix in ['accounting_transactions','current_match_labels','match_training_examples','current_audit_assessments','audit_training_examples']:
        statements.append(f'-- GRANT SELECT ON VIEW {q(suffix)} TO `APP_SERVICE_PRINCIPAL_ID`;')
    volume_parts = store.document_prefix.split('/')[2:5]
    volume = '.'.join('`'+p+'`' for p in volume_parts)
    statements.append(f'-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME {volume} TO `APP_SERVICE_PRINCIPAL_ID`;')
    return '\n\n'.join(statements)


def training_guide():
    return '''Deployment (still four files): replace app.py, main.py, app.yaml and requirements.txt; keep the existing sql-warehouse resource. Run the additive SQL under Workflow setup once as the schema owner, then grant the app SELECT/MODIFY on the three new event tables, SELECT on the workflow views and READ/WRITE VOLUME for evidence uploads. View owners need continuing SELECT on their source tables. No DDL runs at app startup.

The existing fiar_ai_link_review and fiar_review_decisions tables are preserved. Labels in fiar_match_labels are the authority for new review states. The old decision table receives a compatible history copy when possible; a failed copy does not discard a confirmed label. Historical approvals are not silently converted into model-training labels because they may lack exact candidate/version identity. No MERGE, accounting-record writes, or source-table status updates are used.

The match dataset uses TRUE_MATCH=1 and NON_MATCH=0; manual corrections and uploaded evidence are positive labels with explicit provenance. One append-only fact table uses document_type plus structured field snapshots; five document-specific SQL views are provided. Conflicting reviewer labels on the same version and competing positive targets for a document row are excluded. Revisions retain full event history. A rejection is only a negative example for that exact document-row/transaction pair, never for all transactions.

The audit dataset is independent: PASS=0 and FAIL=1 for a specified audit test and criteria version. NEEDS_REVIEW and NOTES_ONLY remain in event history but are excluded from binary training. Conflicting assessments are excluded. Auditors must define the actual tests and policy; a matching document or equal amount does not automatically prove audit compliance.

Uploads go to user_uploads/<upload_id>/<filename> under the existing document volume (20 MiB maximum). A persistent upload record acts as the ingestion handoff; extraction_status=PENDING_EXTRACTION is a backlog marker, not a running job. This app does not train a model or automatically run OCR/extraction. Register/process these upload rows in your existing approved ingestion pipeline when ready. A user must explicitly confirm a manual link; uploading alone never labels a match. If a file upload succeeds but SQL registration fails, retry with the same upload ID; the app verifies bytes before reusing the path. Unregistered files can remain after a failed registration; no file is silently deleted.

A transaction with no candidates appears in Transactions needing evidence; rejecting all its current proposals leaves it ALL_REJECTED and offers document upload/manual linking. HAS_ACCEPTED_EVIDENCE means at least one accepted file, not a complete lifecycle package. Multi-row evidence needs the correct row/page ID. Multiple accepted targets for one document row are flagged for resolution.

Events are retried by stable IDs with INSERT WHERE NOT EXISTS, and reads confirm saved rows. Delta does not enforce unique event IDs, so concurrent identical submissions may still duplicate raw events; latest-per-reviewer views count once. Financial transactions and evidence are rechecked before labels/assessments, but a change immediately after checking cannot be prevented. Stored snapshots identify what was reviewed; later versions do not inherit approval automatically. Audit evidence snapshots record file paths and candidate versions, not immutable copies of source files. Keep versioned source evidence for strict retention.

Training preparation: use the provided training views, authorize agency-data use, adjudicate disputed cases, and split by document AND transaction families/time before model development. document_group_key and transaction_key are supplied for grouping, not an automatic leakage-proof split. Keep a held-out evaluation set; exclude reviewer notes, final labels and other post-decision outcomes from model inputs. Do not auto-retrain or auto-promote from live reviewer clicks. Synthetic/demo events never write to live tables.

This POC limits source candidates to 10,000, current label votes to 25,000, and transaction search matches to 10,000. It fails rather than silently truncates larger datasets. Add server-side paging for larger deployments. Unlinked documents available for manual selection are the saved review repository plus the upload registry; documents not present there must be registered by your existing extraction workflow or uploaded.

App authorization is shared: all users allowed into the app can access the data permitted to its service principal. Restrict app access to authorized reviewers/auditors. UI roles are not a separate auditor access-control system. Existing direct original-document links use viewer permissions; bundle downloads and uploads use app credentials.
'''

class DemoStore(ReviewStore):
    """Synthetic session data; mirrors live workflows without database writes."""
    host = 'https://example.databricks.com'
    namespace = 'workspace.default'
    document_prefix = '/Volumes/workspace/default/fiar-docs'
    source = qualified('workspace.default.fiar_ai_link_review')
    decisions = qualified('workspace.default.fiar_review_decisions')
    invoice_table = qualified('workspace.default.payables_vendor_invoices')
    disbursement_table = qualified('workspace.default.genledger_disbursements')

    def __init__(self, state):
        self.state = state
        if 'workflow_demo_source' not in state:
            state['workflow_demo_source'] = []
            for i, kind, amount in [(1,'invoice','1250.00'),(2,'voucher','500.00'),(3,'sot_detail','960.00')]:
                state['workflow_demo_source'].append(dict(document_id=str(i), document_row_id='0', source_row_key=f'doc-{i}:0',
                    document_type=kind, file_name=f'synthetic-{kind}-{i}.md', storage_uri=self.document_prefix+f'/synthetic/{i}.md',
                    invoice_number=f'INV-{i:04d}', dssn='8522', voucher_number=f'{i:07d}', reference_number=None,
                    contract_number='SPE7M325P1054', doc_amount=amount, system_amount=amount, amount_match='MATCH',
                    target_schema='payables' if i==1 else 'genledger', target_table='vendor_invoices' if i==1 else 'disbursements',
                    target_record_id=str(i), actual_record_json=json_text({'id':i,'amount':amount,'vendor':'Synthetic vendor'}),
                    source_excerpt=f'# Synthetic {kind}\nInvoice INV-{i:04d}\nTotal {amount}', extraction_error=None, review_status='PROPOSED'))
        state.setdefault('workflow_demo_events', {})
        state.setdefault('workflow_demo_legacy', [])
        state.setdefault('workflow_demo_files', {r['storage_uri']:r['source_excerpt'].encode() for r in state['workflow_demo_source']})
        for key in ['match_labels','evidence_uploads','current_match_labels','accounting_transactions']:
            setattr(self,key,self.object_name(key))
        self.audit_assessments_table = self.object_name('audit_assessments')
        from types import SimpleNamespace
        def upload(file_path, contents, overwrite=False):
            if file_path in state['workflow_demo_files'] and not overwrite:
                raise FileExistsError(file_path)
            state['workflow_demo_files'][file_path] = contents.read()
        def download(file_path):
            data = state['workflow_demo_files'][file_path]
            return SimpleNamespace(contents=BytesIO(data),content_length=len(data))
        self.client = SimpleNamespace(files=SimpleNamespace(create_directory=lambda **kw:None,upload=upload,download=download))

    def source_rows(self):
        return list(self.state['workflow_demo_source'])

    def label_events(self):
        return latest_events(self.state['workflow_demo_events'].get(self.match_labels,[]),['candidate_key','snapshot_hash','reviewer_email'])

    def _transactions(self):
        rows = [{k:r[k] for k in ['target_schema','target_table','target_record_id','actual_record_json','system_amount']} for r in self.source_rows()]
        rows.append(dict(target_schema='genledger',target_table='disbursements',target_record_id='100',
            system_amount='750.00',actual_record_json=json_text({'id':100,'amount':'750.00','vendor':'No evidence vendor'})))
        for row in rows:
            row['transaction_key']=transaction_key(row)
        return rows

    def transaction(self, target):
        rows=[r for r in self._transactions() if target_identity(r)==target_identity(target)]
        if len(rows)!=1:
            raise ReviewError('Transaction not found.')
        return rows[0]

    def list_transactions(self, search='', offset=0, limit=20, unresolved_only=True):
        candidates=self.load_review_data()
        rows=[]
        for tx in self._transactions():
            row=dict(tx)
            linked=[r for r in candidates if target_identity(r)==target_identity(tx)]
            row['resolution_status']=resolution_state(tx,linked)
            row['candidate_count']=len(linked)
            if (not unresolved_only or row['resolution_status']!='HAS_ACCEPTED_EVIDENCE') and (not search or search.lower() in json_text(row).lower()):
                rows.append(row)
        priority={'NO_CANDIDATES':0,'ALL_REJECTED':1,'STALE_CANDIDATES':2,'CONFLICT':3,'CANDIDATES_TO_REVIEW':4,'HAS_ACCEPTED_EVIDENCE':5}
        rows.sort(key=lambda r:(priority[r['resolution_status']],target_identity(r)))
        return rows[offset:offset+limit],len(rows)

    def append_event(self, table, event):
        events=self.state['workflow_demo_events'].setdefault(table,[])
        previous=next((e for e in events if e['event_id']==event['event_id']),None)
        if previous:
            if any(str(previous.get(k))!=str(v) for k,v in event.items()):
                raise ReviewError('Submission ID already used with different data.')
            return previous
        saved=dict(event,reviewed_at=datetime.now(timezone.utc).isoformat())
        events.append(saved)
        return saved

    def mirror_legacy(self,row,decision,name,email,notes,event_id):
        if not any(r['decision_id']==event_id for r in self.state['workflow_demo_legacy']):
            self.state['workflow_demo_legacy'].append(dict(row,decision=decision,decision_id=event_id,
                reviewer_name=name,reviewer_email=email,review_comments=notes,reviewed_at=datetime.now(timezone.utc).isoformat()))

    def load_history(self):
        return list(reversed(self.state['workflow_demo_legacy']))

    def training_history(self):
        return list(reversed(self.state['workflow_demo_events'].get(self.match_labels,[])))

    def documents(self):
        docs={str(r['document_id']):dict(r) for r in self.source_rows()}
        for r in self.state['workflow_demo_events'].get(self.evidence_uploads,[]):
            docs[r['document_id']]=dict(r,document_content_hash=r['content_hash'])
        return list(docs.values())

    def audit_assessments(self,tx,context_hash=None):
        return [r for r in reversed(self.state['workflow_demo_events'].get(self.audit_assessments_table,[]))
                if r['transaction_key']==transaction_key(tx) and (context_hash is None or r['audit_context_hash']==context_hash)]
