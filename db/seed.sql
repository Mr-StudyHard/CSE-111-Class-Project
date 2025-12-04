BEGIN TRANSACTION;
PRAGMA foreign_keys = ON;

INSERT INTO users (email) VALUES
  ('alice@example.com'),
  ('bob@example.com'),
  ('chris@example.com'),
  ('dana@example.com');

INSERT INTO genres (tmdb_genre_id, name) VALUES
  (28, 'Action'),
  (12, 'Adventure'),
  (16, 'Animation'),
  (35, 'Comedy'),
  (18, 'Drama'),
  (14, 'Fantasy'),
  (27, 'Horror'),
  (878, 'Science Fiction'),
  (53, 'Thriller'),
  (10765, 'Sci-Fi & Fantasy');

INSERT INTO movies (tmdb_id, title, release_year, runtime_min, overview, poster_path, tmdb_vote_avg, popularity) VALUES
  (603692, 'John Wick: Chapter 4', 2023, 169, 'With the price on his head ever increasing, John Wick uncovers a path to defeating The High Table.', '/vZloFAK7NmvMGKE7VkF5UHaz0I.jpg', 7.7, 1456.8),
  (550, 'Fight Club', 1999, 139, 'A ticking-time-bomb insomniac and a slippery soap salesman channel primal male aggression into a shocking new form of therapy.', '/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg', 8.4, 89.3),
  (157336, 'Interstellar', 2014, 169, 'The adventures of a group of explorers who make use of a newly discovered wormhole to surpass the limitations on human space travel.', '/gEU2QniE6E77NI6lCU6MxlNBvIx.jpg', 8.4, 234.5),
  (299536, 'Avengers: Infinity War', 2018, 149, 'As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos.', '/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg', 8.3, 567.9),
  (27205, 'Inception', 2010, 148, 'Cobb, a skilled thief who commits corporate espionage by infiltrating the subconscious of his targets is offered a chance to regain his old life.', '/ljsZTbVsrQSqZgWeep2B1QiDKuh.jpg', 8.4, 178.2),
  (807, 'Se7en', 1995, 127, 'Two homicide detectives are on a desperate hunt for a serial killer whose crimes are based on the seven deadly sins.', '/6yoghtyTpznpBik8EngEmJskVUO.jpg', 8.3, 92.7);

INSERT INTO shows (tmdb_id, title, first_air_date, last_air_date, overview, poster_path, tmdb_vote_avg, popularity) VALUES
  (1399, 'Game of Thrones', '2011-04-17', '2019-05-19', 'Seven noble families fight for control of the mythical land of Westeros. Friction between the houses leads to full-scale war.', '/1XS1oqL89opfnbLl8WnZY1O1uJx.jpg', 8.4, 312.4),
  (66732, 'Stranger Things', '2016-07-15', NULL, 'When a young boy vanishes, a small town uncovers a mystery involving secret experiments, terrifying supernatural forces, and one strange little girl.', '/x2LSRK2Cm7MZhjluni1msVJ3wDF.jpg', 8.6, 2891.5),
  (82856, 'The Mandalorian', '2019-11-12', NULL, 'After the fall of the Galactic Empire, lawlessness has spread throughout the galaxy. A lone gunfighter makes his way through the outer reaches.', '/eU1i6eHXlzMOlEq0ku1Rzq7Y4wA.jpg', 8.5, 789.3);

-- Seasons
INSERT INTO seasons (show_id, season_number, title, air_date) VALUES
  ((SELECT show_id FROM shows WHERE title='Game of Thrones'), 1, 'Season 1', '2011-04-17'),
  ((SELECT show_id FROM shows WHERE title='Game of Thrones'), 2, 'Season 2', '2012-04-01'),
  ((SELECT show_id FROM shows WHERE title='Stranger Things'), 1, 'Season 1', '2016-07-15'),
  ((SELECT show_id FROM shows WHERE title='Stranger Things'), 2, 'Season 2', '2017-10-27'),
  ((SELECT show_id FROM shows WHERE title='The Mandalorian'), 1, 'Season 1', '2019-11-12'),
  ((SELECT show_id FROM shows WHERE title='The Mandalorian'), 2, 'Season 2', '2020-10-30');

-- Episodes (3 per season)
INSERT INTO episodes (season_id, episode_number, title, air_date, runtime_min) VALUES
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=1), 1, 'Winter Is Coming', '2011-04-17', 62),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=1), 2, 'The Kingsroad', '2011-04-24', 56),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=1), 3, 'Lord Snow', '2011-05-01', 58),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=2), 1, 'The North Remembers', '2012-04-01', 53),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=2), 2, 'The Night Lands', '2012-04-08', 54),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Game of Thrones') AND season_number=2), 3, 'What Is Dead May Never Die', '2012-04-15', 53),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=1), 1, 'The Vanishing of Will Byers', '2016-07-15', 48),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=1), 2, 'The Weirdo on Maple Street', '2016-07-15', 55),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=1), 3, 'Holly, Jolly', '2016-07-15', 51),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=2), 1, 'MADMAX', '2017-10-27', 48),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=2), 2, 'Trick or Treat, Freak', '2017-10-27', 56),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='Stranger Things') AND season_number=2), 3, 'The Pollywog', '2017-10-27', 51),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=1), 1, 'Chapter 1: The Mandalorian', '2019-11-12', 39),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=1), 2, 'Chapter 2: The Child', '2019-11-15', 32),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=1), 3, 'Chapter 3: The Sin', '2019-11-22', 38),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=2), 1, 'Chapter 9: The Marshal', '2020-10-30', 54),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=2), 2, 'Chapter 10: The Passenger', '2020-11-06', 42),
  ((SELECT season_id FROM seasons WHERE show_id = (SELECT show_id FROM shows WHERE title='The Mandalorian') AND season_number=2), 3, 'Chapter 11: The Heiress', '2020-11-13', 35);

-- Movie genres junction
INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Action','Thriller')
WHERE m.title='John Wick: Chapter 4';

INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Drama','Thriller')
WHERE m.title='Fight Club';

INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Science Fiction','Drama')
WHERE m.title='Interstellar';

INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Action','Adventure','Science Fiction')
WHERE m.title='Avengers: Infinity War';

INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Action','Science Fiction')
WHERE m.title='Inception';

INSERT INTO movie_genres (movie_id, genre_id)
SELECT m.movie_id, g.genre_id FROM movies m
JOIN genres g ON g.name IN ('Thriller','Drama')
WHERE m.title='Se7en';

-- Show genres junction
INSERT INTO show_genres (show_id, genre_id)
SELECT s.show_id, g.genre_id FROM shows s
JOIN genres g ON g.name IN ('Drama','Fantasy')
WHERE s.title='Game of Thrones';

INSERT INTO show_genres (show_id, genre_id)
SELECT s.show_id, g.genre_id FROM shows s
JOIN genres g ON g.name IN ('Drama','Science Fiction')
WHERE s.title='Stranger Things';

INSERT INTO show_genres (show_id, genre_id)
SELECT s.show_id, g.genre_id FROM shows s
JOIN genres g ON g.name IN ('Science Fiction','Adventure')
WHERE s.title='The Mandalorian';

-- People
INSERT INTO people (tmdb_person_id, name, profile_path) VALUES
  (287, 'Brad Pitt', '/bradpitt.jpg'),
  (1283, 'Matthew McConaughey', '/mcconaughey.jpg'),
  (976, 'Chris Pratt', '/pratt.jpg'),
  (1223786, 'Pedro Pascal', '/pascal.jpg'),
  (10990, 'Emilia Clarke', '/clarke.jpg'),
  (87545, 'Millie Bobby Brown', '/millie.jpg');

-- Movie cast
INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
SELECT m.movie_id, p.person_id, 'Tyler Durden', 1
FROM movies m, people p
WHERE m.title='Fight Club' AND p.name='Brad Pitt';

INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
SELECT m.movie_id, p.person_id, 'Cooper', 1
FROM movies m, people p
WHERE m.title='Interstellar' AND p.name='Matthew McConaughey';

INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
SELECT m.movie_id, p.person_id, 'Star-Lord', 1
FROM movies m, people p
WHERE m.title='Avengers: Infinity War' AND p.name='Chris Pratt';

-- Show cast
INSERT INTO show_cast (show_id, person_id, character, cast_order)
SELECT s.show_id, p.person_id, 'Daenerys Targaryen', 1
FROM shows s, people p
WHERE s.title='Game of Thrones' AND p.name='Emilia Clarke';

INSERT INTO show_cast (show_id, person_id, character, cast_order)
SELECT s.show_id, p.person_id, 'Eleven', 1
FROM shows s, people p
WHERE s.title='Stranger Things' AND p.name='Millie Bobby Brown';

INSERT INTO show_cast (show_id, person_id, character, cast_order)
SELECT s.show_id, p.person_id, 'Din Djarin', 1
FROM shows s, people p
WHERE s.title='The Mandalorian' AND p.name='Pedro Pascal';

-- Reviews (mixture of movies & shows)
INSERT INTO reviews (user_id, movie_id, rating, content)
SELECT u.user_id, m.movie_id, 9.0, 'Incredible sci-fi odyssey.'
FROM users u, movies m
WHERE u.email='alice@example.com' AND m.title='Interstellar';

INSERT INTO reviews (user_id, movie_id, rating, content)
SELECT u.user_id, m.movie_id, 8.5, 'Mind-bending heist thriller.'
FROM users u, movies m
WHERE u.email='bob@example.com' AND m.title='Inception';

INSERT INTO reviews (user_id, show_id, rating, content)
SELECT u.user_id, s.show_id, 9.2, 'Fantasy at its best (until that last season).'
FROM users u, shows s
WHERE u.email='alice@example.com' AND s.title='Game of Thrones';

INSERT INTO reviews (user_id, show_id, rating, content)
SELECT u.user_id, s.show_id, 8.8, 'Nostalgic sci-fi horror fun.'
FROM users u, shows s
WHERE u.email='chris@example.com' AND s.title='Stranger Things';

INSERT INTO reviews (user_id, movie_id, rating, content)
SELECT u.user_id, m.movie_id, 7.8, 'Stylish action sequences.'
FROM users u, movies m
WHERE u.email='dana@example.com' AND m.title='John Wick: Chapter 4';

-- Discussions
INSERT INTO discussions (user_id, movie_id, title)
SELECT u.user_id, m.movie_id, 'Ending theories'
FROM users u, movies m
WHERE u.email='bob@example.com' AND m.title='Inception';

INSERT INTO discussions (user_id, show_id, title)
SELECT u.user_id, s.show_id, 'Season 2 predictions'
FROM users u, shows s
WHERE u.email='chris@example.com' AND s.title='Stranger Things';

-- Comments
INSERT INTO comments (discussion_id, user_id, content)
SELECT d.discussion_id, u.user_id, 'I think Cobb is still dreaming.'
FROM discussions d, users u
WHERE d.title='Ending theories' AND u.email='alice@example.com';

INSERT INTO comments (discussion_id, user_id, content)
SELECT d.discussion_id, u.user_id, 'No way, the top wobbles!'
FROM discussions d, users u
WHERE d.title='Ending theories' AND u.email='bob@example.com';

INSERT INTO comments (discussion_id, user_id, content)
SELECT d.discussion_id, u.user_id, 'Hoping for more Hopper backstory.'
FROM discussions d, users u
WHERE d.title='Season 2 predictions' AND u.email='dana@example.com';

-- Watchlists
INSERT INTO watchlists (user_id, movie_id, show_id)
SELECT u.user_id, m.movie_id, NULL
FROM users u, movies m
WHERE u.email='alice@example.com' AND m.title='Se7en';

INSERT INTO watchlists (user_id, movie_id, show_id)
SELECT u.user_id, NULL, s.show_id
FROM users u, shows s
WHERE u.email='bob@example.com' AND s.title='Stranger Things';

INSERT INTO watchlists (user_id, movie_id, show_id)
SELECT u.user_id, m.movie_id, NULL
FROM users u, movies m
WHERE u.email='chris@example.com' AND m.title='John Wick: Chapter 4';

COMMIT;

