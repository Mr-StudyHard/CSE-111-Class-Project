from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class MediaItem(Base):
    __tablename__ = "media_items"

    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, nullable=False)
    media_type = Column(String(10), nullable=False)  # 'movie' | 'tv'

    title = Column(String(300), nullable=False)
    overview = Column(String(5000), default="")
    poster_path = Column(String(500))
    backdrop_path = Column(String(500))

    vote_average = Column(Float, default=0.0)
    vote_count = Column(Integer, default=0)
    popularity = Column(Float, default=0.0)

    release_date = Column(String(20))  # 'YYYY-MM-DD' for movies / first_air_date for TV
    genres = Column(String(500))  # comma-separated genre names for simplicity
    original_language = Column(String(10))

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("tmdb_id", "media_type", name="uq_tmdb_media"),
        Index("ix_type_popularity", "media_type", "popularity"),
        Index("ix_type_vote", "media_type", "vote_average"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "media_type": self.media_type,
            "title": self.title,
            "overview": self.overview,
            "poster_path": self.poster_path,
            "backdrop_path": self.backdrop_path,
            "vote_average": self.vote_average,
            "vote_count": self.vote_count,
            "popularity": self.popularity,
            "release_date": self.release_date,
            "genres": self.genres.split(",") if self.genres else [],
            "original_language": self.original_language,
        }


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    # For demo purposes only: storing plaintext for display in the UI table
    password_plain = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {"id": self.id, "email": self.email}
