# app/models.py
from sqlalchemy import Column, BigInteger, String, Boolean, DateTime, Integer
from sqlalchemy.sql import func
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)
    is_deleted = Column(Boolean, nullable=False, default=False)

class Watermark(Base):
    __tablename__ = "watermarks"

    id = Column(Integer, primary_key=True, index=True)
    consumer_id = Column(String(255), nullable=False, unique=True, index=True)
    last_exported_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
