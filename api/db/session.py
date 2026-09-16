"""SQLite connection setup, kept behind SQLAlchemy so swapping to
Postgres later is just a DATABASE_URL change.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base

DATABASE_URL = "sqlite:///./app.db"

engine = create_engine(
    DATABASE_URL,
    # SQLite only allows one thread per connection by default, FastAPI
    # can call a session from different threads.
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
