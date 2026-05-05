from sqlalchemy import Column, Integer, String, Float
from database_analytics import BaseAnalytics

class FuelRecord(BaseAnalytics):
    __tablename__ = "fuel_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(String(50), nullable=False)
    date = Column(String(50))
    machine = Column(String(100))
    fuel_level_be4 = Column(Float)
    fuel_level_aft = Column(Float)
    battery_pole = Column(String(100))
    battery_water = Column(String(100))
    radiator_water = Column(String(100))
    engine_oil = Column(String(100))
    control_light = Column(String(100))
    tech_name = Column(String(200))
    status = Column(String(100))
    app_name = Column(String(200))

