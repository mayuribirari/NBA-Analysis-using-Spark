# NBA-Analysis-using-Spark

The approach followed summarized as follows:

1. Data Import and Grouping: The process begins by importing the necessary libraries and reading the NBA data. The dataframe is then grouped by player name, closest defender, and shot result, and the count of occurrences is aggregated for each group. Additionally, the data is grouped by player name and closest defender, and a fear score is calculated based on the proportion of shots made versus missed.

2. Fear Score Analysis: The fear_playerwise_count dataframe is further grouped by player name, and the minimum fear score is calculated for each player. The closest defender associated with the minimum fear score is selected, representing the player's "most unwanted defender."

3. Comfort Zone Classification: For the second question, the algorithm leverages Spark's VectorAssembler to combine all columns in the initial dataframe into a single column of feature vectors called "features." Silhouette scores are then calculated for different values of k (the number of clusters). The scores are plotted to determine an optimal value for k, and k-means clustering is implemented with k=5.

4. Data Analysis and Joining: Predictions are collected from the clustering results and stored in a new Pandas dataframe. This dataframe is concatenated with another dataframe, keeping only the common columns. Two dataframes are merged based on specific columns, and a new Spark dataframe is created from the merged data. The first 10 rows of this dataframe are displayed.

5. Further Data Manipulation: An empty Spark dataframe is created with a defined schema for player data. Another dataframe is created by filtering a given list of player names. An empty dataframe with a predefined schema is also created using Spark. A temporary view named "NBAData" is generated from the filtered dataframe.

6. Data Aggregation and Analysis: The data is grouped and aggregated by player name and prediction to count the number of successful shots made by each player. Results are displayed in a table. Additionally, the data is grouped and aggregated based on player name and prediction, and the player name, prediction, and total number of shots for each player are selected and displayed.

7. Comfort Zone Analysis: Two queries are joined based on player name and cluster, and a new column called "Hit_rate" is added to the resulting dataframe. SQL queries are executed on the Spark dataframe to select the player name, cluster, and maximum hit rate for each player.

Overall, the approach involves data manipulation, grouping, aggregation, clustering, merging, and SQL querying to analyze NBA data, calculate fear scores, classify comfort zones, and perform player-specific analyses.
