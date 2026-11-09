"""Streamlit entry point for the existing Databricks App."""
import json
import logging
import os
from uuid import uuid4
import pandas as pd
import streamlit as st
from app import ReviewError, document_link, ReviewStore, DemoStore, build_bundle, bundle_signature, key_values, DOC_TYPES, DOCUMENT_FIELDS, transaction_key, record_hash, evidence_context, workflow_setup_sql, training_guide, LABEL_VERSION
from html import escape

st.set_page_config(page_title='FIAR Document Matching Review', page_icon='📋', layout='wide')
st.markdown('<style>\n.stApp {background:#f6f8fc}\n.block-container {padding-top:2rem;max-width:1550px}\n[data-testid="stSidebar"] {background:#edf2f8}\n[data-testid="stMetric"] {background:white;border:1px solid #e0e7f0;border-radius:14px;padding:18px}\n[data-testid="stMetricLabel"] {color:#64748b}\n[data-testid="stMetricValue"] {color:#183451}\n[data-testid="stDataFrame"] {border-radius:12px;overflow:hidden;border:1px solid #e0e7f0}\n.fiar-hero {background:linear-gradient(110deg,#142a46,#244e72);color:white;border-radius:20px;padding:28px 32px;margin-bottom:24px}\n.fiar-hero .eyebrow {font-size:11px;letter-spacing:2px;color:#9ed6de;font-weight:700}\n.fiar-hero h1 {color:white;font-size:32px;margin:8px 0 10px;padding:0}\n.fiar-hero p {color:#d5e2ef;margin:0;font-size:15px}\n.key-values {font-size:13px;background:#fff;border-radius:9px;padding:8px 12px}\n.key-line {display:flex;justify-content:space-between;gap:16px;padding:4px 0;border-bottom:1px solid #f1f5f9;overflow-wrap:anywhere}\n.key-line span {color:#64748b}.key-line strong {color:#183451;text-align:right}\n</style>', unsafe_allow_html=True)
st.markdown('''<div class="fiar-hero"><div class="eyebrow">FIAR · AUDIT EVIDENCE</div>
<h1>Document Matching Review</h1><p>Verify accounting records, approve document links, capture training labels, and assess audit evidence.</p></div>''', unsafe_allow_html=True)

def render_targets(store, demo_mode):
    st.subheader('Target records & evidence bundles')
    st.caption('One row per accounting record. Key values come from the saved accounting snapshot.')
    search = st.text_input('Search target records', placeholder='Record ID, invoice number, vendor…', key='target_search')
    policy = st.radio('Bundle contents', ['Approved documents', 'Approved + pending proposals'], horizontal=True)
    include_pending = policy != 'Approved documents'
    if include_pending:
        st.info('Review bundles include unapproved proposals. Rejected, conflicting and extraction-error links are excluded.')
    filter_key = (search, include_pending)
    if st.session_state.get('target_filter') != filter_key:
        st.session_state['target_page'] = 0
        st.session_state['target_filter'] = filter_key
        st.session_state.pop('prepared_bundle', None)
    page = st.session_state.get('target_page', 0)
    try:
        targets, total = store.list_targets(search, page * 10, 10)
    except Exception:
        logging.exception('Target index read failed')
        st.error('Could not load target records. Check SELECT permission on the saved candidate table.')
        return
    st.caption(f'{total:,} target records · up to 100 documents / 50 MiB per ZIP')
    if not targets:
        if page > 0:
            st.session_state['target_page'] = 0
            st.rerun()
        st.info('No target records found. Unmatched documents remain in the review queue.')
        return
    widths = [2, 4, 1.5, 2]
    for col, label in zip(st.columns(widths), ['TARGET RECORD', 'KEY COLUMNS & VALUES', 'DOCUMENTS', 'DOWNLOAD BUNDLE']):
        col.markdown(f'**{label}**')
    for target in targets:
        identity = (target['target_schema'], target['target_table'], str(target['target_record_id']))
        widget_id = json.dumps(identity)
        with st.container(border=True):
            record_col, keys_col, docs_col, download_col = st.columns(widths)
            record_col.markdown(f"**{identity[0]}.{identity[1]}**")
            record_col.write(f"Record {identity[2]}")
            if target.get('conflict_rows'):
                record_col.caption(f"⚠ {target['conflict_rows']} conflicting links")
            snapshots = list(target.get('accounting_snapshots') or [])
            with keys_col:
                if snapshots:
                    values = key_values(snapshots[0])
                    html = ''.join(f'<div class="key-line"><span>{escape(str(k))}</span><strong>{escape(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) if v is not None else "—")}</strong></div>' for k, v in values.items())
                    st.markdown(f'<div class="key-values">{html}</div>', unsafe_allow_html=True)
                else:
                    st.caption('Accounting snapshot unavailable')
                with st.expander(f'Full accounting record · {len(snapshots)} snapshot(s)'):
                    for snapshot in snapshots:
                        try:
                            st.json(json.loads(snapshot))
                        except (ValueError, TypeError):
                            st.code(str(snapshot), language='json')
                    if len(snapshots) > 1:
                        st.warning('Saved candidates contain different accounting snapshots. All are included in the ZIP.')
            docs_col.metric('Approved', int(target.get('approved_documents', 0)))
            docs_col.caption(f"{target.get('pending_documents', 0)} pending · {target.get('document_count', 0)} total")
            eligible_count = int(target.get('approved_documents', 0)) + (int(target.get('pending_documents', 0)) if include_pending else 0)
            with download_col:
                if st.button('Prepare ZIP', key=f'prepare:{widget_id}', disabled=eligible_count == 0, use_container_width=True):
                    st.session_state.pop('prepared_bundle', None)
                    try:
                        with st.spinner('Collecting supporting documents…'):
                            rows = store.target_candidates(target)
                            signature = bundle_signature(rows, include_pending)
                            data, filename = build_bundle(rows, store.read_document, store.document_prefix, include_pending)
                            if bundle_signature(store.target_candidates(target), include_pending) != signature:
                                raise ReviewError('Links changed while building the ZIP. Prepare it again.')
                            st.session_state['prepared_bundle'] = dict(identity=identity, signature=signature,
                                data=data, filename=filename, include_pending=include_pending)
                    except ReviewError as error:
                        st.warning(str(error))
                    except Exception:
                        reference = str(uuid4())[:8]
                        logging.exception('Evidence bundle failed. Reference %s', reference)
                        st.error(f'Could not download every document. Check volume permissions and file availability. No partial bundle was produced. Reference: {reference}')
                prepared = st.session_state.get('prepared_bundle')
                if prepared and prepared['identity'] == identity:
                    try:
                        current_signature = bundle_signature(store.target_candidates(target), include_pending)
                        if current_signature != prepared['signature']:
                            st.session_state.pop('prepared_bundle', None)
                            st.warning('Links changed. Prepare a fresh ZIP.')
                        else:
                            st.download_button('Download ZIP', data=prepared['data'], file_name=prepared['filename'],
                                               mime='application/zip', key=f'download:{widget_id}',
                                               on_click='ignore', use_container_width=True)
                            st.caption(f"{len(prepared['data']) / 1024:.0f} KB · documents + manifest")
                    except Exception:
                        st.session_state.pop('prepared_bundle', None)
                        st.warning('Could not verify current links. Prepare the ZIP again.')
                elif eligible_count == 0:
                    st.caption('No eligible links yet')
    previous, counter, following = st.columns([1, 4, 1])
    if previous.button('Previous targets', disabled=page == 0):
        st.session_state['target_page'] = page - 1
        st.session_state.pop('prepared_bundle', None)
        st.rerun()
    counter.caption(f'Page {page + 1} of {max(1, (total + 9) // 10)}')
    if following.button('Next targets', disabled=(page + 1) * 10 >= total):
        st.session_state['target_page'] = page + 1
        st.session_state.pop('prepared_bundle', None)
        st.rerun()



# The package still deploys as four files. Database DDL is owner-run, never startup code.
demo_mode=os.getenv('FIAR_DEMO_MODE','false').lower()=='true'
if demo_mode:
    store=DemoStore(st.session_state)
    name,email='Demo Reviewer','demo-reviewer@example.com'
    st.warning('Synthetic demo. Labels, uploads and audit assessments stay in this session.')
else:
    try:
        store=ReviewStore()
    except Exception:
        logging.exception('App configuration failed')
        st.error('Check OAuth credentials and the SQL warehouse resource.')
        st.stop()
    email=st.context.headers.get('x-forwarded-email','')
    name=st.context.headers.get('x-forwarded-preferred-username','') or email

st.sidebar.markdown('### Audit workspace')
st.sidebar.write(f'Reviewer: {email or "Identity unavailable"}')
st.sidebar.caption('Match labels and audit outcomes are recorded separately.')
if st.sidebar.button('Refresh data',use_container_width=True):
    st.session_state.pop('workflow_review_data',None)
    st.session_state.pop('prepared_bundle',None)
    st.rerun()
page=st.radio('Navigate',['Review Queue','Transactions needing evidence','Target Records','Audit assessments','Training labels','Workflow setup'],horizontal=True,label_visibility='collapsed')
if flash:=st.session_state.pop('flash_msg',None):
    st.success(flash)

if page=='Workflow setup':
    st.subheader('Enable label collection and audit review')
    st.write('Run this additive SQL once as the schema owner. Existing review and accounting tables are preserved.')
    st.code(workflow_setup_sql(store),language='sql')
    with st.expander('Deployment, table meanings and model-training guidance',expanded=True):
        st.text(training_guide())
    st.stop()

try:
    if 'workflow_review_data' not in st.session_state:
        st.session_state['workflow_review_data']=store.load_review_data()
    rows=st.session_state['workflow_review_data']
except Exception as error:
    logging.exception('Workflow tables unavailable')
    st.error(str(error) if isinstance(error,ReviewError) else 'Could not read workflow tables.')
    st.info('Open Workflow setup, run the SQL as owner, grant the app access, then Refresh data.')
    st.stop()
for col,label,value in zip(st.columns(4),['Document candidates','Awaiting review','Accepted links','Conflicting labels'],
    [len(rows),sum(r['decision_status']=='PENDING' for r in rows),sum(r['decision_status']=='APPROVED' for r in rows),sum(r['decision_status']=='CONFLICT' for r in rows)]):
    col.metric(label,value)


def submission_id(namespace,signature):
    from app import digest
    key=namespace+':'+digest(signature)
    ids=st.session_state.setdefault('workflow_submission_ids',{})
    if key not in ids:
        ids[key]=str(uuid4())
    return ids[key]


def saved(message):
    st.session_state['flash_msg']=message
    st.session_state.pop('workflow_review_data',None)
    st.session_state.pop('prepared_bundle',None)
    st.rerun()


def error_message(error):
    logging.exception('Workflow operation failed')
    st.error(str(error) if isinstance(error,ReviewError) else 'Could not confirm this operation. Retry the same submission and check the app logs.')


def select_transaction(key,unresolved_default=False):
    search=st.text_input('Search accounting transactions',key=key+':search',placeholder='Record ID, invoice, vendor or amount…')
    unresolved=st.checkbox('Only transactions without accepted evidence',value=unresolved_default,key=key+':unresolved')
    filter_signature=(search,unresolved)
    if st.session_state.get(key+':filter')!=filter_signature:
        st.session_state[key+':page']=0
        st.session_state[key+':filter']=filter_signature
    page_index=st.session_state.get(key+':page',0)
    transactions,total=store.list_transactions(search,page_index*20,20,unresolved)
    st.caption(f'{total:,} accounting transactions match. Evidence coverage means at least one accepted file, not complete audit support.')
    if not transactions:
        if page_index>0:
            st.session_state[key+':page']=0
            st.rerun()
        st.info('No transactions match these filters.')
        return None
    display_cols=['resolution_status','target_schema','target_table','target_record_id','system_amount','candidate_count']
    st.dataframe(pd.DataFrame(transactions).reindex(columns=display_cols),hide_index=True,use_container_width=True)
    previous,counter,following=st.columns([1,4,1])
    if previous.button('Previous transactions',key=key+':prev',disabled=page_index==0):
        st.session_state[key+':page']=page_index-1
        st.rerun()
    counter.caption(f'Page {page_index+1} of {max(1,(total+19)//20)}')
    if following.button('Next transactions',key=key+':next',disabled=(page_index+1)*20>=total):
        st.session_state[key+':page']=page_index+1
        st.rerun()
    mapping={t['transaction_key']:t for t in transactions}
    selected=st.selectbox('Accounting transaction',list(mapping),key=key+':selection',
        format_func=lambda k:f"{mapping[k]['target_table']} / {mapping[k]['target_record_id']} · {mapping[k]['resolution_status']}")
    tx=mapping[selected]
    with st.expander('Actual accounting record',expanded=True):
        st.json(json.loads(tx['actual_record_json']))
    return tx


def render_candidate_labels(tx,candidates):
    st.subheader('Candidate supporting documents')
    if not candidates:
        st.info('The system found no candidate documents. Link an existing document or upload new evidence below.')
        return
    st.dataframe(pd.DataFrame(candidates).reindex(columns=['file_name','document_type','document_row_id','decision_status','doc_amount','system_amount','amount_match']),hide_index=True,use_container_width=True)
    mapping={r['candidate_key']:r for r in candidates}
    selected=st.selectbox('Candidate document',list(mapping),key='transaction:candidate',
        format_func=lambda k:f"{mapping[k]['file_name']} · row {mapping[k].get('document_row_id','0')} · {mapping[k]['decision_status']}")
    row=mapping[selected]
    with st.expander('Candidate evidence',expanded=False):
        st.json({k:row.get(k) for k in DOCUMENT_FIELDS})
        st.code(row.get('source_excerpt') or 'No text excerpt is available.',language=None)
        if not demo_mode and (url:=document_link(store.host,row.get('storage_uri'))):
            st.link_button('Open source document',url)
    valid=row.get('decision_status')!='STALE_ACCOUNTING' and not row.get('extraction_error') and bool(email)
    with st.form('transaction:candidate:form'):
        reason=st.selectbox('Match review reason',['IDENTIFIERS_VERIFIED','AMOUNT_VERIFIED','WRONG_VENDOR','WRONG_IDENTIFIER','WRONG_AMOUNT','WRONG_PERIOD','DUPLICATE_DOCUMENT','INSUFFICIENT_EVIDENCE','OTHER'])
        notes=st.text_area('Candidate review notes',max_chars=4000)
        yes,no=st.columns(2)
        approve=yes.form_submit_button('Label true match',disabled=not valid,use_container_width=True)
        reject=no.form_submit_button('Label non-match',disabled=not valid,use_container_width=True)
    if approve or reject:
        label='TRUE_MATCH' if approve else 'NON_MATCH'
        eid=submission_id('candidate-label',[row['candidate_key'],row['snapshot_hash'],label,reason,notes,email])
        try:
            store.label_candidate(row,label,name,email,notes,eid,reason,row.get('label_origin') or 'PROPOSED')
        except Exception as error:
            error_message(error)
        else:
            saved('Match label recorded for this document/transaction pair.')
    proposals=[r for r in candidates if r['decision_status']=='PENDING' and not r.get('extraction_error')]
    if proposals:
        with st.expander('Reject every remaining proposal'):
            with st.form('transaction:reject-all'):
                reason=st.text_area('Reason all remaining proposals are non-matches',max_chars=4000)
                confirm=st.checkbox('I reviewed every remaining proposal for this transaction.')
                submit=st.form_submit_button('Reject all remaining proposals',disabled=not email)
            if submit:
                if not confirm or not reason.strip():
                    st.warning('Confirm review of every proposal and enter a reason.')
                else:
                    batch=submission_id('reject-batch',[tx['transaction_key'],[(r['candidate_key'],r['snapshot_hash']) for r in proposals],reason,email])
                    failures=[]
                    for row in proposals:
                        from uuid import UUID,uuid5
                        event_id=str(uuid5(UUID(batch),row['candidate_key']+row['snapshot_hash']))
                        try:
                            store.label_candidate(row,'NON_MATCH',name,email,reason,event_id,'OTHER',row.get('label_origin') or 'PROPOSED')
                        except Exception as error:
                            failures.append(str(error))
                    if failures:
                        st.session_state.pop('workflow_review_data',None)
                        st.error('Some labels were saved; other proposals need retry: '+'; '.join(failures[:3]))
                    else:
                        saved('All remaining proposals rejected. Upload or link supporting evidence for this transaction.')


def manual_link_form(document,tx,origin,form_key):
    st.caption('Provide the evidence row/page and verified identifiers. This records a user-supplied positive match; it does not run AI extraction.')
    with st.form(form_key):
        row_id=st.text_input('Document row or page identifier',value='0',help='Use the specific supporting row for SOT/GWA/DCAS documents.')
        cols=st.columns(2)
        fields={}
        for index,field in enumerate(DOCUMENT_FIELDS):
            fields[field]=cols[index%2].text_input(field.replace('_',' ').title(),value=str(document.get(field) or ''))
        notes=st.text_area('Why this document supports the transaction',max_chars=4000)
        submit=st.form_submit_button('Confirm manual true match',type='primary',disabled=not email)
    if submit:
        event_id=submission_id(form_key,[document['document_id'],document.get('storage_uri'),document.get('document_content_hash'),document.get('source_excerpt'),tx['transaction_key'],record_hash(tx['actual_record_json']),row_id,fields,notes,email])
        try:
            store.manual_match(document,tx,row_id,fields,name,email,notes,event_id,origin)
        except Exception as error:
            error_message(error)
        else:
            saved('User-supplied match saved as a positive training label.')


def render_evidence_actions(tx):
    existing_tab,upload_tab=st.tabs(['Link an existing document','Upload new evidence'])
    with existing_tab:
        documents=store.documents()
        search=st.text_input('Find registered document',key='manual:document-search')
        documents=[d for d in documents if not search or search.lower() in json.dumps(d,default=str).lower()]
        if documents:
            mapping={d['document_id']:d for d in documents}
            selected=st.selectbox('Registered source document',list(mapping),
                format_func=lambda k:f"{mapping[k]['file_name']} · {mapping[k]['document_type']} · {k}")
            manual_link_form(mapping[selected],tx,'MANUAL','manual:link')
        else:
            st.info('No registered documents found. Use Upload new evidence.')
    with upload_tab:
        st.caption('PDF, PNG, JPG, Markdown or UTF-8 text · 20 MiB maximum. Uploading registers evidence; you confirm its match separately.')
        uploaded=st.file_uploader('New supporting document',type=['pdf','png','jpg','jpeg','md','txt'])
        kind=st.selectbox('Uploaded document type',DOC_TYPES)
        if st.button('Upload and register document',disabled=not uploaded or not email):
            data=uploaded.getvalue()
            from hashlib import sha256
            upload_id=submission_id('upload',[tx['transaction_key'],record_hash(tx['actual_record_json']),uploaded.name,sha256(data).hexdigest(),kind,email])
            try:
                with st.spinner('Uploading and registering evidence…'):
                    document=store.upload_document(uploaded.name,data,kind,tx,name,email,upload_id)
                    st.session_state['registered_upload']={'transaction_key':tx['transaction_key'],'document':document}
            except Exception as error:
                error_message(error)
        registered=st.session_state.get('registered_upload')
        if registered and registered['transaction_key']==tx['transaction_key']:
            st.success('Evidence registered. It remains pending extraction until your ingestion pipeline processes it.')
            st.write(registered['document']['file_name'])
            manual_link_form(registered['document'],tx,'UPLOAD','upload:confirm-link')


def render_unresolved():
    st.subheader('Transactions needing evidence')
    st.caption('Find missing evidence, review candidate documents, and supply the match the system missed.')
    tx=select_transaction('unresolved',True)
    if tx:
        candidates=store.target_candidates(tx)
        render_candidate_labels(tx,candidates)
        st.divider()
        render_evidence_actions(tx)


def render_audits():
    st.subheader('Audit assessments')
    st.caption('Record a human audit judgment independently of whether a document matches. Define your audit test and criteria version.')
    tx=select_transaction('audit',False)
    if not tx:
        return
    candidates=store.target_candidates(tx)
    evidence,context_hash=evidence_context(tx,candidates)
    if candidates:
        st.dataframe(pd.DataFrame(candidates).reindex(columns=['file_name','document_type','decision_status','amount_match']),hide_index=True,use_container_width=True)
    else:
        st.info('No supporting documents are linked to this transaction.')
    with st.form('audit:assessment'):
        criterion=st.selectbox('Audit test',['SUPPORTING_EVIDENCE','AMOUNT_ACCURACY','AUTHORIZATION','PERIOD_CUTOFF','OTHER'])
        criteria_version=st.text_input('Criteria version',value='draft-v1',help='Replace draft-v1 with the approved version of your audit criteria.')
        outcome=st.selectbox('Audit outcome',['PASS','FAIL','NEEDS_REVIEW','NOTES_ONLY'])
        issue=st.text_input('Issue category',help='Required when the audit outcome is FAIL.')
        notes=st.text_area('Auditor notes',max_chars=4000)
        submit=st.form_submit_button('Save audit assessment',type='primary',disabled=not email)
    if submit:
        event_id=submission_id('audit',[tx['transaction_key'],context_hash,criterion,criteria_version,outcome,issue,notes,email])
        try:
            store.save_audit(tx,candidates,outcome,criterion,criteria_version,issue,notes,name,email,event_id)
        except Exception as error:
            error_message(error)
        else:
            saved('Audit assessment recorded with its accounting and evidence snapshots.')
    assessments=store.audit_assessments(tx)
    if assessments:
        st.dataframe(pd.DataFrame(assessments).reindex(columns=['reviewed_at','outcome','audit_test_code','criteria_version','issue_category','notes','reviewer_email']),hide_index=True,use_container_width=True)
        st.caption('History includes earlier evidence/accounting versions. Training views exclude disputed or non-binary audit outcomes.')


def render_training():
    st.subheader('Human labels & training data')
    st.write('New feedback is recorded as immutable events. Recording labels does not train or fine-tune a model.')
    labels=store.training_history()
    if labels:
        st.dataframe(pd.DataFrame(labels).reindex(columns=['reviewed_at','document_type','document_id','document_row_id','target_table','target_record_id','label','label_origin','reason_code','reviewer_email','notes']),hide_index=True,use_container_width=True)
        mapping={r['event_id']:r for r in labels}
        selected=st.selectbox('Inspect a saved training example',list(mapping))
        st.json(json.loads(mapping[selected]['feature_snapshot_json']))
    else:
        st.info('No human match labels collected yet. Approve/reject a candidate, or provide a manual match to start.')
    st.code(f'''SELECT * FROM {store.namespace}.fiar_match_training_examples;
SELECT * FROM {store.namespace}.fiar_audit_training_examples;
-- Document-type-specific example:
SELECT * FROM {store.namespace}.fiar_invoice_match_training;''',language='sql')
    with st.expander('Dataset rules and deployment guidance'):
        st.text(training_guide())
    with st.expander('Existing decision history'):
        if st.button('Load legacy history'):
            try:
                history=store.load_history()
                if history:
                    st.dataframe(pd.DataFrame(history).reindex(columns=['reviewed_at','decision','reviewer_name','reviewer_email','file_name','review_comments']),hide_index=True,use_container_width=True)
                else:
                    st.info('No legacy decisions recorded.')
            except Exception as error:
                error_message(error)
def render_queue():
    st.subheader('Review queue')
    st.caption('Select a specific candidate to compare its source evidence and accounting record.')
    if not rows:
        st.info('No records found in the saved review table.')
        return
    first, second, third = st.columns(3)
    statuses = first.multiselect('Review status', sorted({str(r.get('review_status')) for r in rows if r.get('review_status')}))
    doctypes = second.multiselect('Document type', sorted({str(r.get('document_type')) for r in rows if r.get('document_type')}))
    amounts = third.multiselect('Amount check', sorted({str(r.get('amount_match')) for r in rows if r.get('amount_match')}))
    search = st.text_input('Find a document or record', placeholder='Invoice, voucher, filename or target record…')
    filtered = [r for r in rows if (not statuses or r.get('review_status') in statuses)
                and (not doctypes or r.get('document_type') in doctypes)
                and (not amounts or r.get('amount_match') in amounts)
                and (not search or search.lower() in ' '.join(str(r.get(k) or '') for k in [
                    'file_name', 'invoice_number', 'voucher_number', 'target_record_id', 'contract_number']).lower())]
    st.caption(f'{len(filtered):,} matching candidates')
    if not filtered:
        st.info('No candidates match these filters.')
        return
    visible = ['review_status', 'document_type', 'file_name', 'invoice_number', 'contract_number',
               'doc_amount', 'system_amount', 'amount_match', 'target_table', 'target_record_id']
    st.dataframe(pd.DataFrame(filtered).reindex(columns=visible), hide_index=True, use_container_width=True,
        column_config={k: k.replace('_', ' ').title() for k in visible}, height=340)
    keys = [r['candidate_key'] for r in filtered]
    if len(set(keys)) != len(keys):
        st.error('Duplicate candidate identities exist in the source table. Deduplicate them before review.')
        return
    records = {r['candidate_key']: r for r in filtered}
    def label(key):
        r = records[key]
        return f"{r.get('file_name')} · row {r.get('document_row_id', '0')} → {r.get('target_table') or 'No target'} / {r.get('target_record_id') or '—'}"
    selected = st.selectbox('Candidate to review', keys, format_func=label)
    rec = records[selected]
    left, right = st.columns(2)
    with left:
        with st.container(border=True):
            st.subheader('Document evidence')
            st.write(rec.get('file_name') or 'Source document')
            st.json({k: rec.get(k) for k in ['invoice_number', 'dssn', 'voucher_number', 'reference_number', 'contract_number', 'doc_amount']})
            if not demo_mode and (url := document_link(store.host, rec.get('storage_uri'))):
                st.link_button('Open original document', url)
            with st.expander('Source excerpt'):
                st.code(str(rec.get('source_excerpt') or rec.get('source_text') or 'No excerpt available.'), language=None)
    with right:
        with st.container(border=True):
            st.subheader('Accounting record')
            st.write(f"{rec.get('target_table') or 'No candidate'} · record {rec.get('target_record_id') or '—'}")
            try:
                st.json(json.loads(rec.get('actual_record_json') or '{}'))
            except (ValueError, TypeError):
                st.code(str(rec.get('actual_record_json') or ''), language='json')
            st.caption(f"Amount check: {rec.get('amount_match') or 'UNKNOWN'} · Accounting amount: {rec.get('system_amount') or '—'}")
    if rec.get('amount_match') != 'MATCH':
        st.warning('Check the document and accounting amounts before approving this candidate.')
    st.subheader('Review decision')
    st.caption(f'Decision will be recorded as {name or "your authenticated reviewer"} ({email or "identity unavailable"}).')
    if not rec.get('target_record_id'):
        st.info('No accounting match was found. Open Transactions needing evidence to select the transaction and supply a manual match.')
    valid = bool(rec.get('target_record_id')) and not rec.get('extraction_error') and bool(email)
    with st.form('review_form'):
        comments = st.text_area('Comments', max_chars=4000, help='Required for rejection or approval with a missing/different amount.')
        approve_col, reject_col, _ = st.columns([1, 1, 3])
        approve = approve_col.form_submit_button('Approve', type='primary', disabled=not valid, use_container_width=True)
        reject = reject_col.form_submit_button('Reject', disabled=not valid, use_container_width=True)
    if approve or reject:
        decision = 'APPROVED' if approve else 'REJECTED'
        signature = (selected, rec['snapshot_hash'], decision, comments.strip())
        if st.session_state.get('submission_signature') != signature:
            st.session_state['submission_signature'] = signature
            st.session_state['submission_id'] = str(uuid4())
        try:
            message=store.save_decision(rec, decision, name, email, comments.strip(), st.session_state['submission_id'])
        except Exception as error:
            logging.exception('Review decision failed')
            st.error(str(error) if isinstance(error, ReviewError) else 'Could not confirm the save. Check the detailed Databricks error and access to your existing decision table.')
        else:
            st.session_state['flash_msg'] = message
            for key in ['workflow_review_data', 'prepared_bundle', 'submission_signature', 'submission_id']:
                st.session_state.pop(key, None)
            st.rerun()


try:
    if page=='Review Queue':
        render_queue()
    elif page=='Transactions needing evidence':
        render_unresolved()
    elif page=='Target Records':
        render_targets(store,demo_mode)
    elif page=='Audit assessments':
        render_audits()
    else:
        render_training()
except Exception as error:
    error_message(error)
