from sqlalchemy import MetaData, Column,\
					ForeignKey, Integer, String, Text, DateTime, Date, PickleType,\
					create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, validates
from sqlalchemy.sql import func

import settings

Base = declarative_base()

class CommonBase(Base):
	__abstract__ = True

	id = Column(Integer, primary_key=True)
	created_on = Column(DateTime, default=func.now())
	updated_on = Column(DateTime, default=func.now(), onupdate=func.now())

	@classmethod
	def create_table(self):
		engine = create_db_engine()
		self.__table__.create(engine)	
		return None

	@classmethod
	def drop_table(self):
		engine = create_db_engine()
		self.__table__.drop(engine)
		return None


class Contact(CommonBase):
	__tablename__ = 'contact'

	name = Column(String(255), nullable=True)
	vcards = relationship("Vcard")
	phones = relationship("Phone")
	emails = relationship("Email")
	addresses = relationship("Address")
	webpages = relationship("Webpage")

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(name=self.name).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return self.name


class Vcard(CommonBase):
	__tablename__ = 'vcard'

	vcard = Column(PickleType, nullable=True)
	contact_id = Column(Integer, ForeignKey('contact.id'))


class Phone(CommonBase):
	__tablename__ = 'phone'

	number = Column(String(255), nullable=True)
	contact_id = Column(Integer, ForeignKey('contact.id'))

	def __init__(self, number, contact_id):
		self.number = number.replace('(','').replace(')','')\
							.replace('-','').replace('.','')
		self.contact_id = contact_id

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(number=self.number).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return self.number


class Email(CommonBase):
	__tablename__ = 'email'

	address = Column(String(255), nullable=True)
	contact_id = Column(Integer, ForeignKey('contact.id'))

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(address=self.address).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return self.address


class Address(CommonBase):
	__tablename__ = 'address'

	address = Column(String(255), nullable=True)
	contact_id = Column(Integer, ForeignKey('contact.id'))

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(address=self.address).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return self.address


class Webpage(CommonBase):
	__tablename__ = 'webpage'

	address = Column(String(255), nullable=True)
	contact_id = Column(Integer, ForeignKey('contact.id'))

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(address=self.address).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return self.address


def create_db_engine(db=settings.DB):
	if db['ENGINE']=='sqlite':
		from sqlite3 import dbapi2 as sqlite
		return create_engine('sqlite+pysqlite:///%s' % (db['NAME']), module=sqlite)

	engine = create_engine('%s://%s:%s@%s/%s' \
			% (db['ENGINE'], db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])\
			, use_native_unicode=False) #, client_encoding='utf8')
	return engine

def create_db_session(db=settings.DB):
	engine = create_db_engine(db)
	Session = sessionmaker(bind=engine)
	session = Session()
	return session

def end_db_session(session):
	session.commit()
	session.close()
	return


from sqlalchemy import exc, event
from sqlalchemy.pool import Pool

@event.listens_for(Pool, "checkout")
def check_connection(dbapi_con, con_record, con_proxy):
	'''Listener for Pool checkout events that pings every connection before using.
	Implements pessimistic disconnect handling strategy. See sqlalchemy docs'''

	cursor = dbapi_con.cursor()
	try:
		cursor.execute("SELECT 1")  # could also be dbapi_con.ping(),
									# not sure what is better
	except exc.OperationalError, ex:
		if ex.args[0] in (2006,   # MySQL server has gone away
						2013,   # Lost connection to MySQL server during query
						2055):  # Lost connection to MySQL server at '%s', system error: %d
			# caught by pool, which will retry with a new connection
			raise exc.DisconnectionError()
		else:
			raise

