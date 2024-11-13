import csv
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, Float, Date, Integer, ForeignKey, UniqueConstraint, MetaData
from sqlalchemy import update, select, delete, insert 

csv_files = ['clean_measure.csv', 'clean_stations.csv']

engine = create_engine('sqlite:///database.db')
meta = MetaData()

with engine.connect() as connection:
    if not engine.has_table('stations') and not engine.has_table('measures'):
        
        measures = Table(
                    'measures', meta,
                    Column('id', Integer, primary_key = True),
                    Column('station', String),
                    Column('date', Date),
                    Column('precip', Float),
                    Column('tobs', Integer),
                    UniqueConstraint('station', 'date', name='uix_station_date')
                )
        
        stations = Table(
                    'stations', meta,
                    Column('id', Integer, primary_key = True),
                    Column('station', String, ForeignKey('measures.station')),
                    Column('latitude', Float),
                    Column('longitude', Float),
                    Column('elevation', Float),
                    Column('name',String),
                    Column('country', String),
                    Column('state', String),
                    UniqueConstraint('latitude', 'longitude', name='uix_latitude_longitude')
                )     

        meta.create_all(engine)
    else:
        measures = Table('measures', meta, autoload_with = engine)
        stations = Table('stations', meta, autoload_with = engine)

    with open(csv_files[0], newline='', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        data = []

        for row in csv_reader:
            data.append({
                    'station': (row['station']),
                    'date' : datetime.strptime(row['date'], '%Y-%m-%d') if row['date'] else None,
                    'precip' : float(row['precip']) if row['precip'] else None,
                    'tobs' : int(row['tobs']) if row['tobs'] else None
            })
        with connection.begin() as transaction:
            try:
                connection.execute(insert(measures).prefix_with('OR IGNORE'), data)
            except Exception as e:
                print(f' Wystapil blad przy dopisywaniu: {e}')
                transaction.rollback()


