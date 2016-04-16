//load the file 
movies =LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID,Title,Genres);
ratings = LOAD '/Spring-2016-input/ratings.dat'  USING PigStorage(':') AS (UserID,MovieID,Rating,Timestamp);
users = LOAD '/Spring-2016-input/users.dat' USING PigStorage(':') AS (UserID,Gender,Age,Occupation,ZipCode);
//filter the data according to the conditions
age20and40 = FILTER users BY (Age < 35) and (Age > 20);
male = FILTER age20and40 BY Gender == 'F';
zipfilter = FILTER male BY ZipCode matches '1.*';

//join movie and rating table to get the action|war movies
comedyDrama = FILTER movies BY Genres == 'Action|War';
moviesJoinRating = JOIN comedyDrama BY MovieID, ratings BY MovieID;
//get all comedyDrama join with users
Afterjoin = foreach moviesJoinRating generate UserID,ratings::MovieID,Rating,Title,Genres;

//group the table together based on the movieId, then calculate the average raings for the group
groupByrating = GROUP Afterjoin BY MovieID;
averageRating = FOREACH groupByrating GENERATE group AS MovieID, AVG(Afterjoin.Rating) AS avgRating;
temp = GROUP averageRating ALL;
//get the movie whose rating is same as minimum rating, faltten can used to reduce the dimendsion
temp1 = FOREACH temp GENERATE FLATTEN(averageRating.(MovieID,avgRating)),MIN(averageRating.$1) AS min;
//TOBAG(averageRating.avgRating) AS bag
lowestRatedMovie = FILTER temp1 BY avgRating==min;


//get the movie with lowest rating
lowestjoinuser = JOIN ratings BY MovieID, lowestRatedMovie BY MovieID;
form = FOREACH lowestjoinuser GENERATE UserID,ratings::MovieID;
// join the lowestratedmovie and filtereduser, then we get the desire user
result1 = JOIN form BY UserID, zipfilter BY UserID;
result2 = FOREACH result1 GENERATE form::ratings::UserID;
dump result2;


(673)
(1010)
(1835)
(2931)
//**************************************QUESTION2
movies =LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID,Title,Genres);
ratings = LOAD '/Spring-2016-input/ratings.dat'  USING PigStorage(':') AS (UserID,MovieID,Rating,Timestamp);
grouped = COGROUP movies BY MovieID, ratings BY MovieID;
table_limit = LIMIT grouped 5;
dump table_limit;


//**************************************QUESTION3
//here we need to difine the type for the parameter. or we can't parse the parameter to udfs
movies =LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID,Title,Genres:chararray);
REGISTER 'my_udfs.py' using jython as myfuncs;
result = FOREACH movies GENERATE Title,myfuncs.format_genere(Genres);
dump result;