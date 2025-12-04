from __future__ import annotations

import os
from typing import Dict, List, Any

import requests


TMDB_BASE = "https://api.themoviedb.org/3"


class TMDbClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise RuntimeError("TMDB_API_KEY is required. Put it in your environment or .env file.")

    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        params = {**(params or {}), "api_key": self.api_key}
        url = f"{TMDB_BASE}{path}"
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    # ----- public helpers -----
    def trending_all(self, window: str = "day", page: int = 1) -> Dict[str, Any]:
        return self._get(f"/trending/all/{window}", {"page": page})

    def top_rated_movies(self, page: int = 1) -> Dict[str, Any]:
        return self._get("/movie/top_rated", {"page": page})

    def top_rated_tv(self, page: int = 1) -> Dict[str, Any]:
        return self._get("/tv/top_rated", {"page": page})

    def search_multi(self, query: str, page: int = 1) -> Dict[str, Any]:
        if not query:
            return {"page": 1, "results": [], "total_pages": 1, "total_results": 0}
        return self._get("/search/multi", {"query": query, "page": page, "include_adult": False})

    @staticmethod
    def normalize(item: Dict[str, Any]) -> Dict[str, Any]:
        media_type = item.get("media_type") or ("movie" if "title" in item else "tv")
        title = item.get("title") or item.get("name") or "Untitled"
        release = item.get("release_date") or item.get("first_air_date") or None
        genre_names = []  # we only have IDs here; skip names or fetch later if needed
        # TMDb usually returns only genre_ids in list endpoints; keep as empty for now
        return {
            "tmdb_id": item.get("id"),
            "media_type": media_type,
            "title": title,
            "overview": item.get("overview"),
            "poster_path": item.get("poster_path"),
            "backdrop_path": item.get("backdrop_path"),
            "vote_average": float(item.get("vote_average") or 0.0),
            "vote_count": int(item.get("vote_count") or 0),
            "popularity": float(item.get("popularity") or 0.0),
            "release_date": release,
            "genres": ",".join(genre_names),
            "original_language": item.get("original_language"),
        }
