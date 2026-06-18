"""Combine all rowsv2_*.jsonl into a single dashboard data file.
- Revenue & jobs aggregations use COMPLETION DATE only (no fallback)
- TrueCanceled: walk sorted (custkey, created_date) — if next event for same
  customer is within 10 days of cancel, mark as Reschedule. Else TrueCanceled.
- Customer Phone (10-digit) is the primary key, falls back to Customer Name,
  falls back to Address|City|Zip for legacy files."""
import os, json, glob, hashlib, gzip
from collections import defaultdict, Counter
from datetime import datetime, timedelta

OUT = os.environ.get('PIPELINE_OUT_DIR') or '/sessions/trusting-kind-noether/mnt/outputs'

# ----- 1. Load all row JSONL files -----
all_rows = []
import re as _re_y
for fp in sorted(glob.glob(f'{OUT}/rowsv2_*.jsonl') + glob.glob(f'{OUT}/rowsv2_*.jsonl.gz')):
    suffix = fp.split('_')[-1].split('.')[0]
    m = _re_y.match(r'(\d{4})', suffix)
    if not m:
        print(f'  skipping unparseable filename: {fp}')
        continue
    yr = int(m.group(1))
    with (gzip.open(fp,'rt') if fp.endswith('.gz') else open(fp)) as f:
        for line in f:
            r = json.loads(line)
            r['_y'] = yr
            all_rows.append(r)
print(f'Loaded {len(all_rows):,} rows from {len(glob.glob(f"{OUT}/rowsv2_*.jsonl"))} year files')

# ----- 2. TrueCanceled detection -----
# For each canceled row, look at the IMMEDIATELY NEXT event with same custkey
# (sorted by created_date). If within 10 days -> Reschedule. Else -> TrueCanceled.
def parse_d(s):
    if not s: return None
    try: return datetime.strptime(s,'%Y-%m-%d')
    except: return None

# Sort rows: rows with no custkey go to a separate bucket
rows_with_ck = [r for r in all_rows if r.get('ck')]
rows_no_ck = [r for r in all_rows if not r.get('ck')]
print(f'  with custkey: {len(rows_with_ck):,}  no key (will be marked unknown): {len(rows_no_ck):,}')

# Sort by (ck, created_date)
rows_with_ck.sort(key=lambda r: (r['ck'], parse_d(r.get('cd')) or datetime.min))

# Walk and mark
true_canceled = 0; reschedule = 0; unknown_canc = 0
for i, r in enumerate(rows_with_ck):
    if r.get('s') != 'Canceled':
        r['_canc_class'] = None
        continue
    # Look for the next row with same custkey
    cancel_d = parse_d(r.get('cd'))
    if not cancel_d:
        r['_canc_class'] = 'TrueCanceled'  # unknown date — count as cancel
        true_canceled += 1
        continue
    next_event = None
    for j in range(i+1, len(rows_with_ck)):
        nr = rows_with_ck[j]
        if nr['ck'] != r['ck']: break
        nd = parse_d(nr.get('cd'))
        if nd and nd > cancel_d:
            next_event = nr
            break
    if next_event:
        nd = parse_d(next_event.get('cd'))
        if nd and (nd - cancel_d).days <= 10:
            r['_canc_class'] = 'Reschedule'
            reschedule += 1
        else:
            r['_canc_class'] = 'TrueCanceled'
            true_canceled += 1
    else:
        r['_canc_class'] = 'TrueCanceled'
        true_canceled += 1

# Rows with no custkey — count cancels as TrueCanceled (can't detect rebook)
for r in rows_no_ck:
    if r.get('s') == 'Canceled':
        r['_canc_class'] = 'TrueCanceled'
        true_canceled += 1
        unknown_canc += 1
    else:
        r['_canc_class'] = None

all_rows = rows_with_ck + rows_no_ck
print(f'  TrueCanceled: {true_canceled:,}  Reschedule: {reschedule:,}  unknown-key cancels (counted as true): {unknown_canc:,}')
print(f'  Reschedule rate: {reschedule/((true_canceled+reschedule) or 1)*100:.1f}% of all cancels')

# ----- 3. Aggregations -----
# Completion-date-based monthly aggregation by market+trade
monthly = defaultdict(lambda: {'jobs':0,'completed':0,'canceled':0,'true_canceled':0,'reschedule':0,'booked':0,
                                'revenue':0.0,'gm_dol':0.0,'mat_cost':0.0,'equip_cost':0.0,'tot_cost':0.0,
                                'labor_cost':0.0,'hours_worked':0.0,'paid_time':0.0,'sold_hours':0.0,
                                'estimates':0.0,'estimates_sold':0.0,'opportunity':0,'members':0,'leads':0,
                                # Pricing aggregations:
                                'jt_total_sum':0.0,'pricebook_sum':0.0,'price_var_sum':0.0,'price_var_n':0})
year_market = defaultdict(lambda: {'jobs':0,'completed':0,'canceled':0,'true_canceled':0,'reschedule':0,
                                    'revenue':0.0,'gm_dol':0.0,'tot_cost':0.0,'hours_worked':0.0,'sold_hours':0.0,
                                    'first_resp_count':0,'first_resp_total':0.0,'opportunity':0,'estimates_sold':0.0,'members':0,'callbacks':0})
job_types = defaultdict(lambda: {'count':0,'rev':0.0,'gm':0.0})
techs = defaultdict(lambda: {'jobs':0,'rev':0.0,'gm':0.0,'hours':0.0,'paid':0.0,'market':None,'jt':Counter(),'tr':Counter(),'years':set(),'price_var_sum':0.0,'price_var_n':0,'pricebook_sum':0.0,'jt_total_sum':0.0})
sold_by = defaultdict(lambda: {'jobs':0,'rev':0.0,'estimates':0,'sold':0.0,'opps':0,'gm':0.0,'price_var_sum':0.0,'price_var_n':0,'pricebook_sum':0.0,'jt_total_sum':0.0})
campaigns = defaultdict(lambda: {'jobs':0,'rev':0.0})
zips = defaultdict(lambda: {'jobs':0,'rev':0.0,'market':'?'})
callbacks_market_trade = defaultdict(lambda: {'count':0,'total':0,'rev_loss_minutes':0})
sameday_market_trade = defaultdict(lambda: {'sameday_rev':0.0,'total_rev':0.0})
membership_jobs = defaultdict(lambda: {'jobs':0,'rev':0.0})
risk_samples = []
RISK_REASONS = {'Parts on backorder':'red','Cust Schedule Out':'yellow','Customer No Show':'yellow',
                'Could Not Reach':'yellow','Tech Schedule Conflict':'red','Permit Issue':'red',
                'Equipment Issue':'red','Manager Approval':'yellow','CSR Error':'yellow','Customer Cancelled':'yellow'}
tech_monthly = defaultdict(lambda: defaultdict(float))
cohort_first = {}  # ck -> {first_year, first_month, first_camp, market, jobs:0, rev:0, trades:set}
jt_monthly = defaultdict(lambda: {'jobs':0,'rev':0.0,'gm':0.0})

for r in all_rows:
    market = r['m']; trade = r['t']
    is_completed = r['s']=='Completed'
    is_canceled = r['s']=='Canceled'
    canc_class = r.get('_canc_class')
    rev = r.get('rv',0)
    completion = r.get('cm')
    created = r.get('cd')

    # Derive completion-month for revenue aggregation
    if is_completed and completion:
        cm_ym = str(completion)[:7]
        if not (len(cm_ym)==7 and cm_ym[:4].isdigit() and cm_ym[4]=='-' and cm_ym[5:7].isdigit()):
            cm_ym = None
    else:
        cm_ym = None

    # Booking month (uses created date)
    cd_ym = created[:7] if created else None

    # Year-month-market-trade — monthly counters
    if cm_ym:
        d = monthly[(cm_ym,market,trade)]
        d['completed'] += 1
        d['revenue'] += rev
        d['gm_dol'] += r.get('gm',0)
        d['mat_cost'] += r.get('mc',0)
        d['equip_cost'] += r.get('ec',0)
        d['tot_cost'] += r.get('tc',0)
        d['labor_cost'] += r.get('lc',0)
        d['hours_worked'] += r.get('hw',0)
        d['paid_time'] += r.get('pt',0)
        d['sold_hours'] += r.get('sh',0)
        d['estimates'] += r.get('es',0)
        d['estimates_sold'] += r.get('so',0)
        if r.get('op'): d['opportunity'] += 1
        if r.get('mb'): d['members'] += 1
        # Pricing aggregations: only count rows where pricebook is non-zero so variance is meaningful
        pb = r.get('pp', 0); pv = r.get('pv', 0); jt_tot = r.get('jt_tot', 0)
        if pb and pb > 0:
            d['jt_total_sum'] += jt_tot
            d['pricebook_sum'] += pb
            d['price_var_sum'] += pv
            d['price_var_n'] += 1
    # Booked uses created date — every job counts toward 'booked' regardless of status
    if cd_ym:
        d = monthly[(cd_ym,market,trade)]
        d['booked'] += 1
        d['jobs'] += 1
        if is_canceled: d['canceled'] += 1
        if canc_class=='TrueCanceled': d['true_canceled'] += 1
        elif canc_class=='Reschedule': d['reschedule'] += 1

    # year_market
    yr = r['_y']
    ymd = year_market[(yr, market)]
    ymd['jobs'] += 1
    if is_completed:
        ymd['completed'] += 1; ymd['revenue'] += rev; ymd['gm_dol'] += r.get('gm',0)
        ymd['tot_cost'] += r.get('tc',0); ymd['hours_worked'] += r.get('hw',0); ymd['sold_hours'] += r.get('sh',0)
        ymd['estimates_sold'] += r.get('so',0)
        if r.get('op'): ymd['opportunity'] += 1
        if r.get('mb'): ymd['members'] += 1
        jt_ = r.get('jt')
        if jt_=='Recall' or 'Return Service' in (jt_ or ''):
            ymd['callbacks'] += 1
    if is_canceled: ymd['canceled'] += 1
    if canc_class=='TrueCanceled': ymd['true_canceled'] += 1
    elif canc_class=='Reschedule': ymd['reschedule'] += 1

    # Job types
    jt_ = r.get('jt') or 'Unknown'
    job_types[(jt_,trade,market)]['count'] += 1
    if is_completed:
        job_types[(jt_,trade,market)]['rev'] += rev
        job_types[(jt_,trade,market)]['gm'] += r.get('gm',0)
        # jt_monthly for drill-down
        if cm_ym:
            jtm = jt_monthly[(cm_ym, market, trade, jt_)]
            jtm['jobs'] += 1
            jtm['rev'] += rev
            jtm['gm'] += r.get('gm',0)

    # Techs
    tname = r.get('tk')
    if tname:
        t = techs[tname]
        t['jobs'] += 1
        if is_completed:
            t['rev'] += rev; t['gm'] += r.get('gm',0)
            t['hours'] += r.get('hw',0); t['paid'] += r.get('pt',0)
            if cm_ym:
                tech_monthly[tname][cm_ym] += rev
            # Pricing per-tech
            pb = r.get('pp',0); pv = r.get('pv',0); jt_tot = r.get('jt_tot',0)
            if pb and pb > 0:
                t['price_var_sum'] += pv
                t['price_var_n'] += 1
                t['pricebook_sum'] += pb
                t['jt_total_sum'] += jt_tot
        t['jt'][jt_] += 1; t['tr'][trade] += 1
        t['years'].add(yr)
        if t['market'] is None: t['market'] = market

    # Sales reps
    sname = r.get('sb')
    if sname:
        s = sold_by[sname]
        s['jobs'] += 1
        if is_completed:
            s['rev'] += rev
            s['estimates'] += int(r.get('es',0))
            s['sold'] += r.get('so',0)
            s['gm'] += r.get('gm',0)
            if r.get('op'): s['opps'] += 1
            # Pricing per-rep
            pb = r.get('pp',0); pv = r.get('pv',0); jt_tot = r.get('jt_tot',0)
            if pb and pb > 0:
                s['price_var_sum'] += pv
                s['price_var_n'] += 1
                s['pricebook_sum'] += pb
                s['jt_total_sum'] += jt_tot

    # Campaigns
    cc = r.get('cc') or 'Unknown'
    campaigns[cc]['jobs'] += 1
    if is_completed: campaigns[cc]['rev'] += rev

    # ZIPs
    z = r.get('zp')
    if z and is_completed:
        zd = zips[z]
        zd['jobs'] += 1; zd['rev'] += rev; zd['market'] = market

    # Callbacks per market+trade
    if jt_=='Recall' or 'Return Service' in (jt_ or ''):
        callbacks_market_trade[(market,trade)]['count'] += 1
        callbacks_market_trade[(market,trade)]['rev_loss_minutes'] += (r.get('hw',0) or 1.5)*60
    if is_completed:
        callbacks_market_trade[(market,trade)]['total'] += 1

    # Same-day velocity: completion date == created date
    if is_completed and rev>0 and created and completion:
        sd = sameday_market_trade[(market,trade)]
        sd['total_rev'] += rev
        if created==completion:
            sd['sameday_rev'] += rev

    # Membership jobs
    if 'Membership' in (jt_ or ''):
        membership_jobs[(yr,market,trade)]['jobs'] += 1
        if is_completed: membership_jobs[(yr,market,trade)]['rev'] += rev

    # Risk samples — TrueCanceled with known reason (not Reschedule)
    if canc_class=='TrueCanceled' and r.get('cr') in RISK_REASONS and len(risk_samples)<80:
        risk_samples.append({'job':r['jn'],'jt':jt_,'market':market,'reason':r['cr'],'severity':RISK_REASONS[r['cr']],'rev_at_risk':rev})

    # Cohort
    ck = r.get('ck')
    if ck:
        if ck not in cohort_first:
            cohort_first[ck] = {'first_year':yr, 'first_month':cd_ym or f'{yr}-01', 'first_camp':r.get('cc') or 'Unknown', 'market':market, 'jobs':0, 'rev':0.0, 'trades':set()}
        f = cohort_first[ck]
        if is_completed:
            f['jobs'] += 1
            f['rev'] += rev
            f['trades'].add(trade)

# ----- 4. Cohort retention curves -----
def months_diff(ym1, ym2):
    if not ym1 or not ym2: return 0
    a=ym1.split('-'); b=ym2.split('-')
    if len(a)<2 or len(b)<2: return -1  # malformed (e.g. 3-digit-year typo) -> skip via d<0
    try:
        return (int(b[0])-int(a[0]))*12 + (int(b[1])-int(a[1]))
    except (ValueError, IndexError):
        return -1

# Build per-cohort cumulative rev curves by tracking completion month - first month for each row
cohort_rev_at = defaultdict(lambda: {0:0,1:0,3:0,6:0,12:0,24:0})
cohort_size = defaultdict(int)
for ck, info in cohort_first.items():
    fm = info['first_month']
    if fm: cohort_size[fm] += 1
for r in all_rows:
    if not r.get('cm') or not r.get('ck') or r.get('s')!='Completed': continue
    ck = r['ck']
    info = cohort_first.get(ck)
    if not info: continue
    fm = info['first_month']
    cm_ym = r['cm'][:7]
    d = months_diff(fm, cm_ym)
    if d<0: continue
    rev = r.get('rv',0)
    for k in [0,1,3,6,12,24]:
        if d <= k:
            cohort_rev_at[fm][k] += rev

cohorts = {fm:{'size':cohort_size[fm],'cum_rev':v} for fm,v in cohort_rev_at.items() if cohort_size[fm]>=10}

# ----- 5. Customer share-of-home -----
share = {'cincy_total':0,'cincy_multi':0,'dayton_total':0,'dayton_multi':0}
for ck,info in cohort_first.items():
    if info['market']=='Cincinnati':
        share['cincy_total'] += 1
        if len(info['trades'])>=2: share['cincy_multi'] += 1
    elif info['market']=='Dayton':
        share['dayton_total'] += 1
        if len(info['trades'])>=2: share['dayton_multi'] += 1

# Whales: top customers by lifetime rev
def cust_anon(ck):
    return 'CUST-'+hashlib.md5(ck.encode()).hexdigest()[:6].upper()
top_cust = sorted(cohort_first.items(), key=lambda x:-x[1]['rev'])[:30]
top_customers = [
    {'id':cust_anon(ck),'rev':round(v['rev'],2),'jobs':v['jobs'],'first_year':v['first_year'],'market':v['market'],'first_camp':v['first_camp']}
    for ck,v in top_cust
]

# ----- 6. Compose final -----
years = sorted(set(r['_y'] for r in all_rows))
out = {
    'meta': {
        'company':'Apollo Home Services',
        'years': years,
        'rows': len(all_rows),
        'unique_customers': len(cohort_first),
        'true_canceled': true_canceled,
        'reschedule': reschedule,
        'reschedule_rate_of_cancels': round(reschedule/((true_canceled+reschedule) or 1)*100, 2) if (true_canceled+reschedule) else 0,
        'data_grain': 'completion-date for revenue; created-date for booked count',
        'cust_key_priority': 'phone -> name -> address',
    },
    'monthly': [{'ym':k[0],'market':k[1],'trade':k[2], **{kk:(round(vv,2) if isinstance(vv,float) else vv) for kk,vv in v.items()}} for k,v in monthly.items()],
    'year_market': [{'year':k[0],'market':k[1], **{kk:(round(vv,2) if isinstance(vv,float) else vv) for kk,vv in v.items()}} for k,v in year_market.items()],
    'job_types': sorted([{'jt':k[0],'trade':k[1],'market':k[2], **v} for k,v in job_types.items() if v['count']>=3], key=lambda x:-x['count'])[:300],
    'techs': sorted([{'name':k,'jobs':v['jobs'],'rev':round(v['rev'],2),'gm':round(v['gm'],2),'hours':round(v['hours'],2),'paid':round(v['paid'],2),'market':v['market'],
                       'top_jt':v['jt'].most_common(1)[0][0] if v['jt'] else None,
                       'top_trade':v['tr'].most_common(1)[0][0] if v['tr'] else None,
                       'years':sorted(list(v['years'])),
                       'pv_avg': round(v['price_var_sum']/v['price_var_n']*100,2) if v['price_var_n'] else None,
                       'pv_n': v['price_var_n'],
                       'pricebook_sum': round(v['pricebook_sum'],2),
                       'jt_total_sum': round(v['jt_total_sum'],2)}
                      for k,v in techs.items() if v['rev']>1000], key=lambda x:-x['rev']),
    'sales_reps': sorted([{'name':k, **{kk:(round(vv,2) if isinstance(vv,float) else vv) for kk,vv in v.items()},
                           'pv_avg': round(v['price_var_sum']/v['price_var_n']*100,2) if v['price_var_n'] else None}
                          for k,v in sold_by.items() if v['rev']>5000], key=lambda x:-x['rev']),
    'campaigns': sorted([{'name':k, **{kk:(round(vv,2) if isinstance(vv,float) else vv) for kk,vv in v.items()}} for k,v in campaigns.items()], key=lambda x:-x['rev']),
    'zips': sorted([{'zip':k, **{kk:(round(vv,2) if isinstance(vv,float) else vv) for kk,vv in v.items()}} for k,v in zips.items() if v['jobs']>=5], key=lambda x:-x['rev'])[:80],
    'cb_market_trade': [{'market':k[0],'trade':k[1], **v} for k,v in callbacks_market_trade.items()],
    'sameday': [{'market':k[0],'trade':k[1],'pct': v['sameday_rev']/v['total_rev']*100 if v['total_rev'] else 0, **{kk:round(vv,2) for kk,vv in v.items()}} for k,v in sameday_market_trade.items()],
    'membership_jobs': [{'year':k[0],'market':k[1],'trade':k[2], **{kk:round(vv,2) if isinstance(vv,float) else vv for kk,vv in v.items()}} for k,v in membership_jobs.items()],
    'risk_samples': risk_samples,
    'tech_monthly': {tn: {ym:round(v,2) for ym,v in by_ym.items()} for tn,by_ym in tech_monthly.items() if sum(by_ym.values())>5000},
    'jt_monthly': sorted([{'ym':k[0],'market':k[1],'trade':k[2],'jt':k[3],'jobs':v['jobs'],'rev':round(v['rev'],0),'gm':round(v['gm'],0)} for k,v in jt_monthly.items()], key=lambda x:-x['rev'])[:2000],
    'cohorts': cohorts,
    'share_of_home': share,
    'cohort': {  # legacy compat
        'count_by_year': dict(Counter([info['first_year'] for info in cohort_first.values()])),
        'rev_by_year': {str(yr): round(sum(info['rev'] for info in cohort_first.values() if info['first_year']==yr),2) for yr in years},
        'lifetime_rev_buckets': dict(Counter([
            '$0-500' if info['rev']<500 else '$500-2K' if info['rev']<2000 else '$2K-5K' if info['rev']<5000 else '$5K-15K' if info['rev']<15000 else '$15K+'
            for info in cohort_first.values()
        ])),
        'lifetime_jobs_buckets': dict(Counter([
            '1 job' if info['jobs']<=1 else '2-3 jobs' if info['jobs']<=3 else '4-5 jobs' if info['jobs']<=5 else '6+ jobs'
            for info in cohort_first.values()
        ])),
        'by_camp_first': [{'camp':camp,'count':c,'total_rev':round(rv,2)} for camp,(c,rv) in {
            camp: (sum(1 for x in cohort_first.values() if x['first_camp']==camp), sum(x['rev'] for x in cohort_first.values() if x['first_camp']==camp))
            for camp in set(x['first_camp'] for x in cohort_first.values())
        }.items()],
        'top_customers': top_customers,
    },
}

with open(f'{OUT}/dashboard_data.json','w') as f:
    json.dump(out, f, separators=(',',':'))
print(f'\nFinal dashboard_data.json: {os.path.getsize(f"{OUT}/dashboard_data.json"):,} bytes')
print(f'  years: {years}')
print(f'  monthly entries: {len(out["monthly"])}')
print(f'  job_types: {len(out["job_types"])}')
print(f'  techs: {len(out["techs"])}')
print(f'  sales_reps: {len(out["sales_reps"])}')
print(f'  cohorts: {len(out["cohorts"])}')
print(f'  unique customers: {out["meta"]["unique_customers"]:,}')
print(f'  TrueCanceled: {true_canceled:,} · Reschedule: {reschedule:,}')

# Quick year x market summary
yr_mkt_summary = defaultdict(lambda: {'jobs':0,'rev':0,'true_canc':0,'resch':0,'booked':0})
for r in out['year_market']:
    k = (r['year'], r['market'])
    yr_mkt_summary[k] = r
print(f'\nYear x Market summary:')
for (yr,mk), r in sorted(yr_mkt_summary.items()):
    print(f"  {yr} {mk:12s}  comp={r['completed']:6d}  rev=${r['revenue']:>13,.0f}  true_canc={r['true_canceled']:5d}  resch={r['reschedule']:5d}")
