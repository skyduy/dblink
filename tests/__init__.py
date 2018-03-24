from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import String, Integer, Date
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50))
    fullname = Column(String(50))
    password = Column(String(12))

    addresses = relationship('Address', order_by='Address.email_address',
                             back_populates='user')


class Address(Base):
    __tablename__ = 'addresses'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship("User", back_populates="addresses")


class BirthInfo(Base):
    __tablename__ = 'birth_info'

    user_id = Column(Integer, primary_key=True)
    birthday = Column(Date, primary_key=True)


def create_table(engine):
    Base.metadata.create_all(engine)


def drop_table(engine):
    Base.metadata.drop_all(engine)
