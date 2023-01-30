import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#URLs 
URL_2016 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2016"
URL_2017 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2017"
URL_2018 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2018"
URL_2019 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2019"
URL_2020 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2020"
URL_2021 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2021"
URL_2022 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2022"
URL_2023 = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=-1&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=2023"


# Requests
response_2016 = requests.get(URL_2016)
response_2017 = requests.get(URL_2017)
response_2018 = requests.get(URL_2018)
response_2019 = requests.get(URL_2019)
response_2020 = requests.get(URL_2020)
response_2021 = requests.get(URL_2021)
response_2022 = requests.get(URL_2022)
response_2023 = requests.get(URL_2023)

# Load as JSON
data_2016 = json.loads(response_2016.text)
data_2017 = json.loads(response_2017.text)
data_2018 = json.loads(response_2018.text)
data_2019 = json.loads(response_2019.text)
data_2020 = json.loads(response_2020.text)
data_2021 = json.loads(response_2021.text)
data_2022 = json.loads(response_2022.text)
data_2023 = json.loads(response_2023.text)

datas = [data_2016, data_2017, data_2018, data_2019, data_2020, data_2021, data_2022, data_2023]

# Create a connection to SQLite
engine = create_engine("sqlite:///found_objects.db")

# Data model ORM Sqlite
Base = declarative_base()

class FoundObject(Base):
    __tablename__ = "found_objects"
    recordid = Column(String, primary_key=True)

    # Mapping
    type = Column(String)
    description = Column(String)
    date_found = Column(DateTime)

# Create table
Base.metadata.create_all(engine)

# Create a session to add data 
Session = sessionmaker(bind=engine)
session = Session()

# Add datas
for data in datas:
    for item in data["records"]:
        date_found = datetime.strptime(item["fields"]["date"], "%Y-%m-%dT%H:%M:%S+00:00")
        record = FoundObject(
            recordid=item["recordid"], 
            type=item["fields"]["gc_obo_type_c"],
            description=item["fields"]["gc_obo_nature_c"],
            date_found=date_found
        )
        session.add(record)


# Save the modification
session.commit()

# Close the session
session.close()
