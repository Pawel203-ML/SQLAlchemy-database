from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, Float, Date, Integer, ForeignKey, MetaData
from sqlalchemy import update, select, delete, insert 

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
        )     

        meta.create_all(engine)