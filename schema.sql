CREATE TABLE IF NOT EXISTS movies (
  movieId INTEGER PRIMARY KEY,
  title TEXT,
  genres TEXT
);

CREATE TABLE IF NOT EXISTS ratings (
  userId INTEGER,
  movieId INTEGER,
  rating REAL,
  timestamp BIGINT
);

CREATE TABLE IF NOT EXISTS movie_ratings_merged (
  userId INTEGER,
  movieId INTEGER,
  rating REAL,
  timestamp BIGINT,
  title TEXT,
  genres TEXT
);
