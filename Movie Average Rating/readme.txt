Data source : https://grouplens.org/datasets/movielens/20m/

Find average rating of a movie.

In map phase generate movieId, rating as values, then in reduce phase for every movieID sum the ratings and divide 
by the total ratings count. 
