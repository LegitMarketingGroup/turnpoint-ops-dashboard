"""Multi-tenant daily ingest orchestrator for TurnPoint (51 brands).

Adapts A's single-tenant daily_ingest.py to fan out across 51 brand
dashboards under LegitMarketingGroup/turnpoint. One commit per day with all
52 changed index.html files (51 brands + HoldCo rollup).

Flow:
  1. step_gmail_pull()          - same as A; downloads ST CSV attachments
  2. step_classify_and_split()  - detect kind+month per CSV; split into per-tenant slices
  3. step_run_all_tenants()     - for each tenant: stream slice + merge + combine + embed
  4. step_rebuild_holdco()      - aggregate per-tenant build_summary.json -> HoldCo index.html
  5. step_freshness_check()     - fail if no tenant's last_data_day advanced
  6. step_commit_push()         - single commit with all 52 changed index.html files
  7. step_idempotency_check()   - exit 0 with skip message if no new attachments

Env vars:
  WORK_DIR    - root of pipeline_state cache (workflow restores 'tenants/' here)
                expected layout:
                  $WORK_DIR/tenants/{tid}/pipeline_state/
                  $WORK_DIR/pipeline/*.py
                  $WORK_DIR/run/TENANT_REGISTRY.json
                  $WORK_DIR/template/index.html
                  $WORK_DIR/build_one_tenant.py
                  $WORK_DIR/split_source_csv.py
  REPO_DIR    - local clone of LegitMarketingGroup/turnpoint repo
  GMAIL_OAUTH_CLIENT_ID, _CLIENT_SECRET, _REFRESH_TOKEN - Gmail API creds
  GITHUB_TOKEN_FOR_PUSH (or GITHUB_TOKEN) - repo push auth
  GMAIL_LOOKBACK_HOURS=72   - Gmail search window
  FORCE_REEMBED=1           - bypass idempotency + skip-on-no-email
  SKIP_GMAIL=1              - reuse files already in inbox/ (testing)
  SKIP_PUSH=1               - run pipeline, don't commit/push (testing)
  TENANT_SUBSET=tid1,tid2   - restrict to these tenant_ids (debug)

Exit codes:
  0 success / no-op
  2 no Gmail messages (clean skip)
  3 reserved (combine_v3 fail per-tenant - not fatal)
  5 git push failed
  6 stale data (no tenant advanced)
  1 other error
"""
import os, sys, json, subprocess, datetime, shutil, glob, re, time
from pathlib import Path

WORK_DIR    = os.environ.get('WORK_DIR', '/sessions/trusting-kind-noether/mnt/outputs/pipeline_state')
PIPE_DIR    = os.environ.get('PIPE_DIR') or os.path.dirname(os.path.abspath(__file__))
REPO_DIR    = os.environ.get('REPO_DIR', '/tmp/tp_run')
INBOX       = os.path.join(WORK_DIR, 'inbox')
SLICES_DIR  = os.path.join(WORK_DIR, 'slices')
TENANTS_DIR = os.path.join(WORK_DIR, 'tenants')
REGISTRY_PATH = os.path.join(WORK_DIR, 'run', 'TENANT_REGISTRY.json')
TEMPLATE_PATH = os.path.join(WORK_DIR, 'template', 'index.html')

SKIP_GMAIL  = os.environ.get('SKIP_GMAIL', '0') == '1'
SKIP_PUSH   = os.environ.get('SKIP_PUSH', '0') == '1'
TENANT_SUBSET = [s.strip() for s in os.environ.get('TENANT_SUBSET', '').split(',') if s.strip()]

REPO_URL    = os.environ.get('TURNPOINT_REPO_URL',
                             'https://github.com/LegitMarketingGroup/turnpoint.git')
today_iso   = datetime.date.today().strftime('%Y-%m-%d')


def log(msg, *, level='INFO'):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] [{level}] {msg}', flush=True)


def run(cmd, cwd=None, env=None, check=True, capture=False, timeout=None):
    pretty = ' '.join(cmd) if isinstance(cmd, list) else cmd
    log(f'$ {pretty[:200]}')
    res = subprocess.run(cmd if isinstance(cmd, list) else cmd.split(),
                         cwd=cwd, env=env, check=False,
                         capture_output=capture, text=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f'  exit {res.returncode}', level='ERROR')
        if capture:
            log(res.stdout or ''); log(res.stderr or '', level='ERROR')
        sys.exit(1)
    return res


def load_registry():
    if not os.path.exists(REGISTRY_PATH):
        log(f'Registry missing: {REGISTRY_PATH}', level='ERROR'); sys.exit(1)
    with open(REGISTRY_PATH) as f: reg = json.load(f)
    if TENANT_SUBSET:
        reg = [t for t in reg if t['tenant_id'] in TENANT_SUBSET]
        log(f'TENANT_SUBSET active: {len(reg)} tenant(s)')
    return reg


def step_gmail_pull():
    if SKIP_GMAIL:
        log(f'SKIP_GMAIL=1 - reusing {INBOX}/')
        return [p for p in glob.glob(f'{INBOX}/*') if os.path.isfile(p)]
    os.makedirs(INBOX, exist_ok=True)
    for p in glob.glob(f'{INBOX}/*'):
        if os.path.isfile(p): os.remove(p)
    log(f'Calling gmail_pull.py -> {INBOX}/')
    lookback = os.environ.get('GMAIL_LOOKBACK_HOURS', '72')
    res = run(['python3', os.path.join(PIPE_DIR, 'gmail_pull.py'),
               INBOX, '--mark-source-subject', '--lookback-hours', lookback],
              capture=True, check=False)
    if res.returncode == 2:
        log(f'No matching Gmail messages in last {lookback}h.', level='WARN')
        log(res.stderr or '', level='WARN')
        if os.environ.get('FORCE_REEMBED','').strip().lower() in ('1','true','yes'):
            log('FORCE_REEMBED=1 - continuing with existing state.', level='WARN')
            return []
        sys.exit(2)
    if res.returncode != 0:
        log(f'gmail_pull failed: {res.stderr}', level='ERROR'); sys.exit(1)
    log(res.stderr or '')
    return [p for p in glob.glob(f'{INBOX}/*') if os.path.isfile(p)]


def detect_month(filename):
    """Detect YYYYMM target from ST filename '..._Dated MM_01_YY - MM_DD_YY.csv'."""
    base = os.path.basename(filename)
    m = re.search(r'(\d{2})[_/-](\d{2})[_/-](\d{2})\D+(\d{2})[_/-](\d{2})[_/-](\d{2})', base)
    if m:
        return f'20{m.group(6)}{m.group(4)}'  # end-date month wins
    m2 = re.search(r'(\d{2})[_/-](\d{2})[_/-](\d{2})\D*\.csv$', base, re.IGNORECASE)
    if m2:
        return f'20{m2.group(3)}{m2.group(1)}'
    return None


def classify_kind(filename):
    bl = os.path.basename(filename).lower().replace('-', ' ').replace('_', ' ')
    if 'completed' in bl:        return 'jobs_completed'
    elif 'calls'   in bl:        return 'calls'
    elif 'campaign' in bl or 'subcategory' in bl: return 'campaign'
    elif 'jobs'    in bl:        return 'jobs_created'
    return None


def step_classify_and_split(paths):
    if not paths:
        log('No attachments to split.')
        return {}
    os.makedirs(SLICES_DIR, exist_ok=True)
    split_script = os.path.join(WORK_DIR, 'split_source_csv.py')
    if not os.path.exists(split_script):
        alt = os.path.join(os.path.dirname(WORK_DIR), 'split_source_csv.py')
        if os.path.exists(alt): split_script = alt
        else:
            log(f'split_source_csv.py missing at {split_script}', level='ERROR'); sys.exit(1)
    classified = {}
    for p in paths:
        kind = classify_kind(p)
        if kind is None:
            log(f'Unrecognized: {os.path.basename(p)}', level='WARN'); continue
        slice_kind = 'jobs' if kind.startswith('jobs') else ('calls' if kind=='calls' else 'campaign')
        yyyymm = detect_month(p) or datetime.date.today().strftime('%Y%m')
        out_dir = os.path.join(SLICES_DIR, slice_kind, yyyymm)
        os.makedirs(out_dir, exist_ok=True)
        log(f'Splitting {os.path.basename(p)} -> {slice_kind}/{yyyymm}/')
        env = {**os.environ, 'TENANT_REGISTRY_PATH': REGISTRY_PATH}
        res = run(['python3', split_script, slice_kind, p, out_dir], capture=True, env=env)
        produced = [os.path.basename(f).split('_')[0]
                    for f in glob.glob(os.path.join(out_dir, '*_*.csv'))]
        classified[(kind, yyyymm)] = produced
        log(f'  -> {len(produced)} tenant slices')
    return classified


def step_run_all_tenants(registry, classified):
    builder = os.path.join(WORK_DIR, 'build_one_tenant.py')
    if not os.path.exists(builder):
        log(f'build_one_tenant.py missing at {builder}', level='ERROR'); sys.exit(1)
    summary = {'success': [], 'fail': [], 'combine_v3_fail': []}
    t_total = time.time()
    for i, t in enumerate(registry, 1):
        tid = t['tenant_id']; slug = t['url_slug']
        t0 = time.time()
        log(f'[{i}/{len(registry)}] {slug} ({tid})')
        env = {**os.environ,
               'KEEP_STATE': '1',
               'PIPELINE_OUT_DIR': os.path.join(TENANTS_DIR, tid, 'pipeline_state'),
               'TENANT_ID': tid,
               'TP_V2_ROOT': WORK_DIR}
        try:
            res = subprocess.run(['python3', builder, tid],
                                 env=env, check=False, capture_output=True,
                                 text=True, timeout=300)
        except subprocess.TimeoutExpired:
            log(f'  TIMEOUT (>300s) for {slug}', level='ERROR')
            summary['fail'].append(tid); continue
        if res.returncode != 0:
            log(f'  FAIL rc={res.returncode} ({time.time()-t0:.1f}s) stderr tail: {res.stderr[-400:]}', level='ERROR')
            summary['fail'].append(tid); continue
        if 'PASS=True' not in (res.stdout or ''):
            summary['combine_v3_fail'].append(tid)
            log(f'  combine_v3 did not PASS - proceeding but flagged', level='WARN')
        summary['success'].append(tid)
        built = os.path.join(WORK_DIR, 'build', 'turnpoint', slug, 'index.html')
        if not os.path.exists(built):
            log(f'  built index.html missing at {built}', level='ERROR')
            summary['fail'].append(tid); continue
        repo_dest = os.path.join(REPO_DIR, slug)
        os.makedirs(repo_dest, exist_ok=True)
        shutil.copy(built, os.path.join(repo_dest, 'index.html'))
        log(f'  OK ({time.time()-t0:.1f}s) -> {slug}/index.html')
    log(f'Done: {len(summary["success"])}/{len(registry)} OK '
        f'({len(summary["fail"])} fail, {len(summary["combine_v3_fail"])} v3-soft-fail) '
        f'in {time.time()-t_total:.0f}s')
    return summary


def step_rebuild_holdco(registry):
    rows = []
    for t in registry:
        tid = t['tenant_id']; slug = t['url_slug']; display = t['display_name']
        tier = t.get('polish_tier', 2)
        summ_path = os.path.join(TENANTS_DIR, tid, 'pipeline_state', 'build_summary.json')
        if not os.path.exists(summ_path):
            log(f'No build_summary for {slug} - skipping HoldCo row', level='WARN'); continue
        with open(summ_path) as f: s = json.load(f)
        rows.append({'slug': slug, 'display': display, 'tier': tier,
                     'rev': s.get('rev_2026_ytd', 0), 'jobs': s.get('jobs_2026_ytd', 0)})
    rows.sort(key=lambda r: -r['rev'])
    body_rows = []
    for r in rows:
        tier_lbl = f'Tier-{r["tier"]}'
        body_rows.append(
            f'<tr><td class="brand"><a href="./{r["slug"]}/">{r["display"]}</a> '
            f'<span class="tier">{tier_lbl}</span></td>'
            f'<td class="num">{r["rev"]:,.0f}</td>'
            f'<td class="num">{r["jobs"]:,}</td></tr>'
        )
    html = (
        '<!doctype html><html><head><meta charset="utf-8"><title>TurnPoint HoldCo Operations</title>'
        '<style>:root{--ink:#111;--ink-mute:#666;--hairline:#e5e5e5;--bg:#fff;'
        '--mono:ui-monospace,SFMono-Regular,Menlo,monospace}'
        'body{margin:0;padding:24px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;'
        'color:var(--ink);background:var(--bg)}'
        '.wrap{max-width:1400px;margin:0 auto}'
        'h1{font-size:24px;margin:0 0 8px 0;font-weight:600}'
        '.sub{color:var(--ink-mute);margin-bottom:32px}'
        'table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:13px}'
        'th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--hairline)}'
        'th{font-weight:600;color:var(--ink-mute);font-size:11px;text-transform:uppercase;letter-spacing:0.5px}'
        'td.num{text-align:right;font-variant-numeric:tabular-nums}'
        '.tier{font-family:var(--mono);font-size:10px;color:var(--ink-mute);margin-left:8px;'
        'padding:2px 6px;border:1px solid var(--hairline);border-radius:3px}'
        'a{color:#08294d;text-decoration:none;font-weight:500} a:hover{text-decoration:underline}'
        '.footer{max-width:1400px;margin:32px auto 24px;padding:24px;font-size:11px;'
        'color:var(--ink-mute);font-family:var(--mono);border-top:1px solid var(--hairline)}'
        '</style></head><body><div class="wrap">'
        '<h1>TurnPoint HoldCo Operations</h1>'
        f'<div class="sub">{len(rows)} brands across the HoldCo &middot; YTD 2026</div>'
        '<table><thead><tr><th>Brand</th><th class="num">YTD Revenue ($)</th><th class="num">YTD Jobs</th></tr></thead>'
        '<tbody>' + ''.join(body_rows) + '</tbody></table>'
        f'<div class="footer">TurnPoint HoldCo rollup &middot; Built {today_iso} &middot; '
        'Password protected. Confidential.</div></div></body></html>'
    )
    out = os.path.join(REPO_DIR, 'index.html')
    with open(out, 'w', encoding='utf-8') as f: f.write(html)
    log(f'HoldCo rebuilt: {len(rows)} brands, {os.path.getsize(out):,} bytes')


def snapshot_last_data_day(registry):
    snap = {}
    for t in registry:
        tid = t['tenant_id']; slug = t['url_slug']
        idx = os.path.join(REPO_DIR, slug, 'index.html')
        if not os.path.exists(idx): continue
        try:
            with open(idx) as f: html = f.read()
            m = re.search(r'"last_data_day":"(\d{4}-\d{2}-\d{2})"', html)
            if m: snap[tid] = m.group(1)
        except Exception: pass
    return snap


def step_freshness_check(registry, prev_snapshot):
    if os.environ.get('SKIP_FRESHNESS_CHECK','').strip().lower() in ('1','true','yes'):
        log('SKIP_FRESHNESS_CHECK=1 - bypassing.', level='WARN'); return
    if os.environ.get('FORCE_REEMBED','').strip().lower() in ('1','true','yes'):
        log('FORCE_REEMBED=true - bypassing freshness check.', level='WARN'); return
    advanced = 0; total = 0
    for t in registry:
        tid = t['tenant_id']; slug = t['url_slug']
        idx = os.path.join(REPO_DIR, slug, 'index.html')
        if not os.path.exists(idx): continue
        total += 1
        try:
            with open(idx) as f: html = f.read()
            m = re.search(r'"last_data_day":"(\d{4}-\d{2}-\d{2})"', html)
            cur = m.group(1) if m else ''
            prev = prev_snapshot.get(tid, '')
            if cur and (not prev or cur > prev): advanced += 1
        except Exception: pass
    log(f'Freshness: {advanced}/{total} tenants advanced last_data_day')
    if total > 0 and advanced == 0:
        log('STALE DATA: no tenant advanced. Failing.', level='ERROR')
        sys.exit(6)


def step_clone_or_pull():
    pat = os.environ.get('GITHUB_TOKEN_FOR_PUSH') or os.environ.get('GITHUB_TOKEN')
    if not pat:
        log('No GITHUB_TOKEN_FOR_PUSH / GITHUB_TOKEN - cannot clone/push.', level='ERROR'); sys.exit(1)
    auth_url = REPO_URL.replace('https://', f'https://x-access-token:{pat}@')
    if os.path.exists(REPO_DIR):
        log(f'Removing stale {REPO_DIR}'); shutil.rmtree(REPO_DIR, ignore_errors=True)
    log('Cloning turnpoint repo')
    run(['git', 'clone', '--depth=20', auth_url, REPO_DIR])
    run(['git', 'config', 'user.email', 'turnpoint-bot@legitmarketinggroup.com'], cwd=REPO_DIR)
    run(['git', 'config', 'user.name',  'turnpoint-daily-ingest'], cwd=REPO_DIR)


def step_idempotency_check():
    if os.environ.get('FORCE_REEMBED','').strip().lower() in ('1','true','yes'):
        log('FORCE_REEMBED=1 - bypassing idempotency.', level='WARN'); return
    res = run(['git', 'log', '--oneline', '-30'], cwd=REPO_DIR, capture=True)
    marker = f'[tp-ingest] {today_iso}'
    if marker in res.stdout:
        log(f'Today already on main ({marker}). Skipping.', level='WARN'); sys.exit(0)


def step_commit_push(summary):
    if SKIP_PUSH:
        log('SKIP_PUSH=1 - done (no push)'); return
    res = run(['git', 'status', '--short'], cwd=REPO_DIR, capture=True)
    if not res.stdout.strip():
        log('No diff vs main - nothing to commit.', level='WARN'); sys.exit(0)
    run(['git', 'add', '-A'], cwd=REPO_DIR)
    n_ok = len(summary.get('success', []))
    n_fail = len(summary.get('fail', []))
    msg = f'[tp-ingest] {today_iso}: refreshed {n_ok} brands' + (f' ({n_fail} fail)' if n_fail else '')
    run(['git', 'commit', '-m', msg], cwd=REPO_DIR)
    res = run(['git', 'push', 'origin', 'main'], cwd=REPO_DIR, capture=True, check=False)
    if res.returncode != 0:
        log('git push failed', level='ERROR'); log(res.stderr or '', level='ERROR'); sys.exit(5)
    log(f'Pushed: {msg}')


def main():
    log(f'=== TurnPoint daily ingest {today_iso} ===')
    log(f'WORK_DIR={WORK_DIR}  REPO_DIR={REPO_DIR}  PIPE_DIR={PIPE_DIR}')

    registry = load_registry()
    log(f'Registry: {len(registry)} tenants')

    step_clone_or_pull()
    step_idempotency_check()

    prev_snap = snapshot_last_data_day(registry)
    log(f'Pre-run snapshot: {len(prev_snap)} tenants have a recorded last_data_day')

    paths = step_gmail_pull()
    log(f'Found {len(paths)} attachment(s)')
    force = os.environ.get('FORCE_REEMBED','').strip().lower() in ('1','true','yes')
    if not paths and not force:
        log('No attachments and no FORCE_REEMBED - nothing to do.', level='WARN'); sys.exit(0)

    classified = step_classify_and_split(paths)
    if not classified and not force:
        log('No slices produced - nothing to do.', level='WARN'); sys.exit(0)

    summary = step_run_all_tenants(registry, classified)
    step_rebuild_holdco(registry)
    step_freshness_check(registry, prev_snap)
    step_commit_push(summary)
    log(f'=== Done: {len(summary["success"])} OK, {len(summary["fail"])} fail ===')


if __name__ == '__main__':
    main()
