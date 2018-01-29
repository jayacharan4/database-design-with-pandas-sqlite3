# database-design-with-pandas-sqlite3

I have used the http://www.retrosheet.org/gamelogs/index.html csv dataset to work.
I have create a empty database with the sqlite3 loaded all of those game_logs samples csv into a Dataframe with Pandas.
Created a dictionary of dataframes and converted all of the dataframes in the dictionary into a list of tables in the database.
The objective of the project is to leverage the property of Normalization using PRIMARY KEYS and FOREIGN KEYS which cant be applied to a dataframe.
