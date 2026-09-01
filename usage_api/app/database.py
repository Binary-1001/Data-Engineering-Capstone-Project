import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

DB_URL = (
    'postgresql://postgres:postgres@localhost:15432/wtc_analytics'
    if environment == 'dev'
    else 'postgresql://postgres:postgres@postgres:5432/wtc_analytics'
)

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)


def get_db():
    """Yields a database session and closes it after the request completes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
