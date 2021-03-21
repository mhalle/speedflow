import json
import sys
from dataclasses import dataclass
from typing import List
import typer
import sqlite3
import dateutil.tz
import datetime
import sqlite_utils


app = typer.Typer()

BBOX = '42.367222,-71.273417;42.281275,-71.151347'

def get_here_url(api_key, bbox):
    return f'https://traffic.ls.hereapi.com/traffic/6.3/flow.json?apiKey={api_key}&bbox={bbox}'

@dataclass
class TMC:
    label: str
    dir: str
    len: str
    PC: str


@dataclass
class FI:
    tmc: TMC
    speed: float
    capped: float
    freeFlow: float
    jamFactor: float
    confidence: float

@dataclass
class SegmentLocation:
    mid: str
    LI: str
    label: str

@dataclass
class RW:
    rid: SegmentLocation
    time: str
    intersections: List[FI]




def parseRW(rw):
    return RW(
            rid=SegmentLocation(rw['mid'], rw['LI'], rw['DE']), 
            time=rw['PBT'],
            intersections= [parseFI(x) for x in rw['FIS'][0]['FI']])

def parseFI(fi):
    return FI(
        tmc=parseTMC(fi['TMC']),
        speed=fi['CF'][0]['SU'],
        capped=fi['CF'][0]['SP'],
        freeFlow=fi['CF'][0]['FF'],
        jamFactor=fi['CF'][0]['JF'],
        confidence=fi['CF'][0]['CN'],
    )
def parseTMC(tmc):
    return TMC(
        label=tmc['DE'],
        dir=tmc['QD'],
        len=tmc['LE'],
        PC=tmc['PC']
    )

class HereFlowInfo:
    def __init__(self, data):
        self.data = data
        rws = data['RWS'][0]
        self.meta = {
            'creationTime': data['CREATED_TIMESTAMP'],
            'version': data['VERSION'],
            'TY': rws['TY'],
            'mapVersion': rws['MAP_VERSION'],
            'tableId': rws['TABLE_ID'],
            'units': rws['UNITS']
        }
        self.roadways = [parseRW(x) for x in rws['RW']]


def get_here_flow_info(api_key, bbox):
    import urllib.request
    here_url = get_here_url(api_key, bbox)
    data = urllib.request.urlopen(here_url)

    h = HereFlowInfo(json.load(data))
    return h

def db_insert(dbname, h, round_to_minutes):
    db = sqlite_utils.Database(dbname)
    for r in h.roadways:
        routepk = db['routes'].upsert({
                        'label': r.rid.label,
                        'mid': r.rid.mid,
                        'li': r.rid.LI
                        }, pk='li').last_pk

        date, time = utc_to_local_date_time(r.time, round_to_minutes)

        timeid = db['datetimes'].lookup({
            'utctime': r.time,
            'date': date,
            'time': time,
        })

        for i in r.intersections:
            unique = f'{r.time}_{r.rid.LI}_{i.tmc.PC}_{i.tmc.dir}'
            unique_int = f'{i.tmc.dir}{i.tmc.PC}'
            intpk = db['intersections'].upsert({
                    'id': unique_int,
                    'label': i.tmc.label,
                    'route': routepk,
                    'direction': i.tmc.dir,
                    'length': i.tmc.len,
                    'pc': i.tmc.PC
            }, pk='id', foreign_keys=[['route', 'routes', 'li']]).last_pk

            db['flow_'].insert({
                'datetime': timeid,
                'route': routepk,
                'intersection': intpk,
                'capped': i.capped,
                'speed': i.speed,
                'freeFlow': i.freeFlow,
                'jamFactor': i.jamFactor,
                'confidence': i.confidence
            }, pk=None, foreign_keys=[['route', 'routes', 'li'], 
                                      ['intersection', 'intersections', 'id'],
                                      ['datetime', 'datetimes', 'id']])

    
    if 'flow_fts' in db.table_names(fts5=True):
        db['flow_fts'].rebuild_fts()
                                      

@app.command()
def log_current_flow(dbname: str, api_key: str, bbox: str, round: int):
    h = get_here_flow_info(api_key, bbox)
    db_insert(dbname, h, round)

@app.command()
def build_view(dbname: str):
    db = sqlite_utils.Database(dbname)

    db.create_view('flow', '''
        select
            flow_.rowid as rowid,
            datetimes.date || "T" || datetimes.time as datetime,
            datetimes.date as date,
            datetimes.time as time,
            routes.label as route,
            intersections.label as intersection,
            intersections.pc as intersectionCode,
            intersections.direction as direction,
            speed,
            freeFlow,
            jamFactor,
            confidence
            from flow_ 
                join routes on flow_.route = routes.li
                join intersections on flow_.intersection = intersections.id
                join datetimes on flow_.datetime = datetimes.id
        ''', replace=True)

@app.command()
def build_fts(dbname: str):
    db = sqlite_utils.Database(dbname)
    try:
        db['flow_fts'].drop()
    except:
        pass

    db.executescript('''
        create virtual table flow_fts using FTS5 (
            date,
            time,
            route,
            intersection,
            content='flow');
            '''
        )
    db.executescript('''
        insert into flow_fts(rowid, date, time, route, intersection)
        select rowid, date, time, route, intersection from flow;
        ''')



@app.command()
def create_indices(dbname: str):
    db = sqlite_utils.Database(dbname)
    db['flow_'].create_index(['datetime', 'route', 'intersection'], unique=True, if_not_exists=True)
    db['datetimes'].create_index(['date'], if_not_exists=True)
    db['datetimes'].create_index(['time'], if_not_exists=True)
    db['routes'].create_index(['label'], if_not_exists=True)
    db['intersections'].create_index(['label'], if_not_exists=True)
    db['flow_'].create_index(['route'], if_not_exists=True)
    db['flow_'].create_index(['intersection'], if_not_exists=True)
    db['flow_'].create_index(['datetime'], if_not_exists=True)

def round_time(dt, minutes):
    dateDelta = datetime.timedelta(minutes=minutes)
    roundTo = dateDelta.total_seconds()

    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def utc_to_local_date_time(utcstr, round_to_minutes):
    to_zone = dateutil.tz.gettz('America/New_York')
    utc = datetime.datetime.strptime(utcstr, "%Y-%m-%dT%H:%M:%S%z")
    local = utc.astimezone(dateutil.tz.gettz('America/New_York'))
    rounded = round_time(local, round_to_minutes)
    return [rounded.date().isoformat(), rounded.strftime('%H:%M')]


if __name__ == '__main__':
    app()

