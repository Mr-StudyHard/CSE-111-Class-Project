-- Q1 Search movies and shows by title substring
-- :keyword='star'
SELECT *
FROM (
    SELECT 'movie' AS target_type, movie_id AS target_id, title, release_year, tmdb_vote_avg
    FROM movies
    WHERE lower(title) LIKE lower('%' || :keyword || '%')
    UNION ALL
    SELECT 'show' AS target_type, show_id AS target_id, title, first_air_date, tmdb_vote_avg
    FROM shows
    WHERE lower(title) LIKE lower('%' || :keyword || '%')
) AS results
ORDER BY (tmdb_vote_avg IS NULL), tmdb_vote_avg DESC, title;

-- Q2 Movie details with average user rating and TMDb average plus genres
-- :movie_id=1
SELECT m.title,
       m.release_year,
       m.tmdb_vote_avg AS tmdb_average,
       AVG(r.rating) AS user_average,
       GROUP_CONCAT(g.name, ', ') AS genres
FROM movies m
LEFT JOIN reviews r ON r.movie_id = m.movie_id
LEFT JOIN movie_genres mg ON mg.movie_id = m.movie_id
LEFT JOIN genres g ON g.genre_id = mg.genre_id
WHERE m.movie_id = :movie_id
GROUP BY m.movie_id;

-- Q3 Show details with season count and average rating
-- :show_id=1
SELECT s.title,
       s.first_air_date,
       s.last_air_date,
       s.tmdb_vote_avg AS tmdb_average,
       COUNT(DISTINCT se.season_id) AS season_count,
       AVG(r.rating) AS user_average
FROM shows s
LEFT JOIN seasons se ON se.show_id = s.show_id
LEFT JOIN reviews r ON r.show_id = s.show_id
WHERE s.show_id = :show_id
GROUP BY s.show_id;

-- Q4 Episode listing for a show ordered by season and episode
-- :show_id=1
SELECT se.season_number,
       se.title AS season_title,
       ep.episode_number,
       ep.title AS episode_title,
       ep.air_date,
       ep.runtime_min
FROM seasons se
JOIN episodes ep ON ep.season_id = se.season_id
WHERE se.show_id = :show_id
ORDER BY se.season_number, ep.episode_number;

-- Q5 Top 5 movies by average user rating with at least 2 reviews
SELECT m.title,
       AVG(r.rating) AS avg_user_rating,
       COUNT(r.review_id) AS review_count
FROM movies m
JOIN reviews r ON r.movie_id = m.movie_id
GROUP BY m.movie_id
HAVING COUNT(r.review_id) >= 2
ORDER BY avg_user_rating DESC
LIMIT 5;

-- Q6 Top 5 shows by average user rating with at least 2 reviews
SELECT s.title,
       AVG(r.rating) AS avg_user_rating,
       COUNT(r.review_id) AS review_count
FROM shows s
JOIN reviews r ON r.show_id = s.show_id
GROUP BY s.show_id
HAVING COUNT(r.review_id) >= 2
ORDER BY avg_user_rating DESC
LIMIT 5;

-- Q7 Genre distribution for movies
SELECT g.name,
       COUNT(mg.movie_id) AS movie_count
FROM genres g
LEFT JOIN movie_genres mg ON mg.genre_id = g.genre_id
GROUP BY g.genre_id
ORDER BY movie_count DESC, g.name;

-- Q8 Genre distribution for shows
SELECT g.name,
       COUNT(sg.show_id) AS show_count
FROM genres g
LEFT JOIN show_genres sg ON sg.genre_id = g.genre_id
GROUP BY g.genre_id
ORDER BY show_count DESC, g.name;

-- Q9 Titles (movies + shows) with no user reviews yet
SELECT 'movie' AS target_type, m.title
FROM movies m
LEFT JOIN reviews r ON r.movie_id = m.movie_id
WHERE r.review_id IS NULL
UNION ALL
SELECT 'show', s.title
FROM shows s
LEFT JOIN reviews r ON r.show_id = s.show_id
WHERE r.review_id IS NULL
ORDER BY target_type, title;

-- Q10 Most-discussed titles by total comments
SELECT target_type,
       target_title,
       SUM(comment_count) AS total_comments
FROM (
    SELECT 'movie' AS target_type,
           m.title AS target_title,
           COUNT(c.comment_id) AS comment_count
    FROM discussions d
    JOIN movies m ON m.movie_id = d.movie_id
    LEFT JOIN comments c ON c.discussion_id = d.discussion_id
    GROUP BY m.movie_id
    UNION ALL
    SELECT 'show',
           s.title,
           COUNT(c.comment_id)
    FROM discussions d
    JOIN shows s ON s.show_id = d.show_id
    LEFT JOIN comments c ON c.discussion_id = d.discussion_id
    GROUP BY s.show_id
) AS aggregated
GROUP BY target_type, target_title
ORDER BY total_comments DESC
LIMIT 10;

-- Q11 Insert movie into watchlist
-- :user_id=1, :movie_id=1
INSERT INTO watchlists (user_id, movie_id, show_id)
VALUES (:user_id, :movie_id, NULL);

-- Q12 Insert show into watchlist
-- :user_id=2, :show_id=1
INSERT INTO watchlists (user_id, movie_id, show_id)
VALUES (:user_id, NULL, :show_id);

-- Q13 Delete from watchlist
-- :user_id=1, :movie_id=1
DELETE FROM watchlists
WHERE user_id = :user_id
  AND movie_id = :movie_id;

-- Q14 Insert review for movie
-- :user_id=1, :movie_id=2, :rating=8.2, :content='Great film'
INSERT INTO reviews (user_id, movie_id, rating, content)
VALUES (:user_id, :movie_id, :rating, :content);

-- Q15 Update review for movie
-- :review_id=1, :rating=9.5, :content='Updated thoughts'
UPDATE reviews
SET rating = :rating,
    content = :content
WHERE review_id = :review_id
  AND movie_id IS NOT NULL;

-- Q16 Delete review for movie
-- :review_id=1
DELETE FROM reviews
WHERE review_id = :review_id
  AND movie_id IS NOT NULL;

-- Q17 Insert discussion for show
-- :user_id=1, :show_id=1, :title='Favorite episode?'
INSERT INTO discussions (user_id, movie_id, show_id, title)
VALUES (:user_id, NULL, :show_id, :title);

-- Q18 Insert comment into a discussion
-- :discussion_id=1, :user_id=2, :content='Loved episode 3!'
INSERT INTO comments (discussion_id, user_id, content)
VALUES (:discussion_id, :user_id, :content);

-- Q19 Delete empty discussions (no comments)
DELETE FROM discussions
WHERE discussion_id NOT IN (
    SELECT DISTINCT discussion_id FROM comments
);

-- Q20 Admin add genre (upsert)
-- :tmdb_genre_id=500, :name='Documentary'
INSERT INTO genres (tmdb_genre_id, name)
VALUES (:tmdb_genre_id, :name)
ON CONFLICT(tmdb_genre_id) DO UPDATE SET name = excluded.name;

-- Q21 Insert or ignore person by TMDb id
-- :tmdb_person_id=12345, :name='Sample Actor', :profile_path='/sample.jpg'
INSERT INTO people (tmdb_person_id, name, profile_path)
VALUES (:tmdb_person_id, :name, :profile_path)
ON CONFLICT(tmdb_person_id) DO UPDATE SET
    name = excluded.name,
    profile_path = excluded.profile_path;

-- Q22 Attach cast to movie
-- :movie_id=1, :person_id=1, :character='Hero', :cast_order=1
INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
VALUES (:movie_id, :person_id, :character, :cast_order)
ON CONFLICT(movie_id, person_id) DO UPDATE SET
    character = excluded.character,
    cast_order = excluded.cast_order;

-- Q23 Attach cast to show
-- :show_id=1, :person_id=2, :character='Villain', :cast_order=2
INSERT INTO show_cast (show_id, person_id, character, cast_order)
VALUES (:show_id, :person_id, :character, :cast_order)
ON CONFLICT(show_id, person_id) DO UPDATE SET
    character = excluded.character,
    cast_order = excluded.cast_order;

-- Q24 List top N cast for a movie ordered by cast_order
-- :movie_id=1, :limit=5
SELECT p.name, mc.character, mc.cast_order
FROM movie_cast mc
JOIN people p ON p.person_id = mc.person_id
WHERE mc.movie_id = :movie_id
ORDER BY mc.cast_order
LIMIT :limit;

-- Q25 List top N cast for a show ordered by cast_order
-- :show_id=1, :limit=5
SELECT p.name, sc.character, sc.cast_order
FROM show_cast sc
JOIN people p ON p.person_id = sc.person_id
WHERE sc.show_id = :show_id
ORDER BY sc.cast_order
LIMIT :limit;

-- Q26 Compare user avg rating vs TMDb avg per movie
SELECT m.title,
       m.tmdb_vote_avg AS tmdb_average,
       AVG(r.rating) AS user_average,
       (AVG(r.rating) - m.tmdb_vote_avg) AS delta
FROM movies m
LEFT JOIN reviews r ON r.movie_id = m.movie_id
GROUP BY m.movie_id
ORDER BY delta DESC;

-- Q27 Average user rating per genre (movies only)
SELECT g.name,
       AVG(r.rating) AS avg_rating
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.genre_id
JOIN reviews r ON r.movie_id = mg.movie_id
GROUP BY g.genre_id
ORDER BY avg_rating DESC;

-- Q28 Users who reviewed at least three titles
SELECT u.email,
       COUNT(r.review_id) AS review_count
FROM users u
JOIN reviews r ON r.user_id = u.user_id
GROUP BY u.user_id
HAVING COUNT(r.review_id) >= 3
ORDER BY review_count DESC;

-- Q29 Watchlist counts per title (movies + shows)
SELECT target_type,
       target_title,
       COUNT(*) AS watchlist_count
FROM (
    SELECT 'movie' AS target_type,
           m.title AS target_title
    FROM watchlists w
    JOIN movies m ON m.movie_id = w.movie_id
    WHERE w.movie_id IS NOT NULL
    UNION ALL
    SELECT 'show',
           s.title
    FROM watchlists w
    JOIN shows s ON s.show_id = w.show_id
    WHERE w.show_id IS NOT NULL
) AS listing
GROUP BY target_type, target_title
ORDER BY watchlist_count DESC;

-- Q30 Recently added titles (by created_at within last 7 days)
SELECT 'movie' AS target_type, title, created_at
FROM movies
WHERE created_at >= datetime('now', '-7 days')
UNION ALL
SELECT 'show', title, created_at
FROM shows
WHERE created_at >= datetime('now', '-7 days')
ORDER BY created_at DESC;

