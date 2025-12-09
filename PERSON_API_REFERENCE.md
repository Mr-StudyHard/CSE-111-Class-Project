# Person API Quick Reference

## Endpoint

```
GET /api/people/<person_id>
```

## Example Request

```bash
curl http://localhost:5000/api/people/1
```

## Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `person_id` | integer | Internal database ID |
| `tmdb_person_id` | integer | TMDb person ID |
| `name` | string | Full name |
| `profile_path` | string | TMDb image path |
| `profile_image_url` | string | Full URL to thumbnail (w185) |
| `profile_image_large_url` | string | Full URL to large image (h632) |
| `birthday` | string | Birth date (YYYY-MM-DD) |
| `deathday` | string\|null | Death date if applicable |
| `place_of_birth` | string | Birthplace |
| `biography` | string | Full biography text |
| `imdb_id` | string | IMDb identifier |
| `instagram_id` | string | Instagram handle |
| `twitter_id` | string | Twitter handle |
| `facebook_id` | string | Facebook identifier |
| `social_links` | object | Constructed URLs for social platforms |
| `movies` | array | Filmography - movies |
| `shows` | array | Filmography - TV shows |

## Social Links Object

```json
{
  "social_links": {
    "imdb": "https://www.imdb.com/name/nm0001234",
    "instagram": "https://www.instagram.com/username",
    "twitter": "https://twitter.com/username",
    "facebook": "https://www.facebook.com/username"
  }
}
```

## Movie Object (in filmography)

```json
{
  "movie_id": 5,
  "title": "Fight Club",
  "release_year": 1999,
  "poster_path": "/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg",
  "poster_url": "https://image.tmdb.org/t/p/w185/...",
  "character": "Tyler Durden",
  "cast_order": 1
}
```

## Show Object (in filmography)

```json
{
  "show_id": 12,
  "title": "Breaking Bad",
  "first_air_date": "2008-01-20",
  "poster_path": "/ggFHVNu6YYI5L9pCfOacjizRGt.jpg",
  "poster_url": "https://image.tmdb.org/t/p/w185/...",
  "character": "Walter White",
  "cast_order": 1
}
```

## Error Responses

### 404 Not Found
```json
{
  "error": "Person not found"
}
```

## Updated Movie/Show Cast Format

The `/api/movie/<id>` and `/api/show/<id>` endpoints now return enhanced cast objects:

```json
{
  "top_cast": [
    {
      "person_id": 1,
      "name": "Brad Pitt",
      "profile_path": "/kU3B75TyRiCgE270EyZnHjfivoq.jpg",
      "profile_url": "https://image.tmdb.org/t/p/w185/...",
      "character": "Tyler Durden",
      "cast_order": 1
    }
  ]
}
```

Use `person_id` to link to `/api/people/<person_id>` for full details.

