import duckdb, json, datetime as dt
from zoneinfo import ZoneInfo
from collections import defaultdict
con=duckdb.connect('sel.duckdb')
con.execute("create or replace table clips as select * from read_csv_auto('clips.csv.gz', timestampformat='%Y-%m-%d %H:%M:%S.%f%z')")
con.execute("create or replace table nas as select * from read_csv_auto('nas.csv.gz')")
con.execute("create or replace table quar as select * from read_csv_auto('quar.csv')")
con.execute("create or replace table streams as select * from read_csv_auto('streams.csv')")
print(con.execute("describe clips").fetchall())
rows=con.execute("""select c.stream_id,c.tz,epoch(c.clip_start_at),epoch(c.clip_end_at),q.clip_id is not null from clips c left join quar q on q.clip_id=c.id
 where c.clip_end_at>c.clip_start_at and c.clip_start_at>='2020-01-01' and c.clip_end_at<'2028-01-01' order by 1,3""").fetchall()
per=defaultdict(lambda: defaultdict(list))  # stream -> date -> [(a,b,clean)]
tzs={}
for sid,tz,a,b,qq in rows:
    z=ZoneInfo(tz); tzs[sid]=tz
    t=a
    while t<b:
        lt=dt.datetime.fromtimestamp(t,z); d=lt.date()
        nxt=dt.datetime.combine(d+dt.timedelta(days=1),dt.time(),z).timestamp()
        e=min(b,nxt); per[sid][d].append((t,e,not qq)); t=e
def union(iv):
    s=0; cur=None
    for a,b in sorted(iv):
        if cur is None or a>cur[1]:
            if cur: s+=cur[1]-cur[0]
            cur=[a,b]
        else: cur[1]=max(cur[1],b)
    if cur: s+=cur[1]-cur[0]
    return s
out=[]
for sid,days in per.items():
    clean={d:union([(a,b) for a,b,c in v if c]) for d,v in days.items()}
    reg={d:union([(a,b) for a,b,c in v]) for d,v in days.items()}
    lo,hi=min(days),max(days)
    def best(m):
        bs=None;bd=None;d=lo-dt.timedelta(days=13)
        while d<=hi:
            s=sum(m.get(d+dt.timedelta(days=i),0) for i in range(14))
            if bs is None or s>bs: bs,bd=s,d
            d+=dt.timedelta(days=1)
        return bd,bs
    bd,bs=best(clean); rd,rs=best(reg)
    z=ZoneInfo(tzs[sid])
    ks=bd-dt.timedelta(days=7); ke=bd+dt.timedelta(days=21)
    out.append(dict(stream_id=sid,tz=tzs[sid],first_day=str(lo),last_day=str(hi),days_recorded=sum(1 for v in reg.values() if v>0),
      best_start=str(bd),best_end_incl=str(bd+dt.timedelta(days=13)),best_hours=round(bs/3600,1),
      keep_start=str(ks),keep_end_excl=str(ke),
      keep_start_utc=dt.datetime.combine(ks,dt.time(),z).astimezone(dt.timezone.utc).isoformat(),
      keep_end_utc=dt.datetime.combine(ke,dt.time(),z).astimezone(dt.timezone.utc).isoformat(),
      registered_winner=str(rd), total_hours=round(sum(reg.values())/3600,1)))
json.dump(sorted(out,key=lambda x:x['stream_id']),open('selection.json','w'),indent=1)
print(len(out),"streams")
