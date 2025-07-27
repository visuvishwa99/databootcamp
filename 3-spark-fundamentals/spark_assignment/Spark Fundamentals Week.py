{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "|            match_id|player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|\n",
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "|71d79b23-4143-435...|      taterbase|                    5|           5|            12537|   13383|                1|                       3|           0|                               98|             NULL|               2|                      3|          0|                              26|            NULL|                  4|          false|        PT14.81149S|                 6|                     4|                       255|                       28|                       0|                        0|                          0|                              0|                               0|                          0|                               0|                              0|                 13|                   1|                         0|      1|      1|\n",
      "|71d79b23-4143-435...| SuPeRSaYaInG0D|                   18|          18|           131943|  132557|                2|                       3|           0|                                2|             NULL|               1|                      3|          0|                              76|            NULL|                  7|          false|      PT11.2990845S|                 7|                     3|        350.58792304992676|                       49|                       1|                       45|                          0|                              0|                               0|                          0|                               0|                              0|                 18|                   2|                         0|      0|      0|\n",
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "only showing top 2 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/07/27 03:10:35 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.\n"
     ]
    }
   ],
   "source": [
    "\"\"\"\n",
    "Task1 | match_details\n",
    "-> a row for every players performance in a match\n",
    "\n",
    "\"\"\"\n",
    "\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import expr, col,count\n",
    "spark = SparkSession.builder.appName(\"Jupyter\").getOrCreate()\n",
    "\n",
    "\n",
    "match_details_df = spark.read.option(\"header\", \"true\").csv(\"/home/iceberg/data/match_details.csv\")\n",
    "\n",
    "player_performances = match_details_df.select(\"*\")\n",
    "\n",
    "# Display the result\n",
    "player_performances.show(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+\n",
      "|            match_id|               mapid|is_team_game|         playlist_id|     game_variant_id|is_match_over|     completion_date|match_duration|game_mode|map_variant_id|\n",
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+\n",
      "|11de1a94-8d07-416...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-22 00:00:...|          NULL|     NULL|          NULL|\n",
      "|d3643e71-3e51-43e...|cb914b9e-f206-11e...|       false|d0766624-dbd7-453...|257a305e-4dd3-41f...|         true|2016-02-14 00:00:...|          NULL|     NULL|          NULL|\n",
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+\n",
      "only showing top 2 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "\"\"\"\n",
    "Task2 | matches\n",
    " -> a row for every match\n",
    "\n",
    "\"\"\"\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import expr, col,count\n",
    "spark = SparkSession.builder.appName(\"Jupyter\").getOrCreate()\n",
    "\n",
    "\n",
    "matches_df = spark.read.option(\"header\", \"true\").csv(\"/home/iceberg/data/matches.csv\")\n",
    "\n",
    "\n",
    "# Display the result\n",
    "matches_df.show(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----+----------+\n",
      "|  medal_id|sprite_uri|sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification|description|name|difficulty|\n",
      "+----------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----+----------+\n",
      "|2315448068|      NULL|       NULL|      NULL|              NULL|               NULL|        NULL|         NULL|          NULL|       NULL|NULL|      NULL|\n",
      "|3565441934|      NULL|       NULL|      NULL|              NULL|               NULL|        NULL|         NULL|          NULL|       NULL|NULL|      NULL|\n",
      "+----------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----+----------+\n",
      "only showing top 2 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "\"\"\"\n",
    "Task3 | medals\n",
    " -> a row for every medal type\n",
    "\n",
    "\"\"\"\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import expr, col,count\n",
    "spark = SparkSession.builder.appName(\"Jupyter\").getOrCreate()\n",
    "\n",
    "\n",
    "medals = spark.read.option(\"header\", \"true\").csv(\n",
    "    \"/home/iceberg/data/medals.csv\"\n",
    ")\n",
    "\n",
    "\n",
    "# Display the result\n",
    "medals.show(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+------+---------+-------+\n",
      "|            match_id|player_gamertag|  name|game_mode|did_win|\n",
      "+--------------------+---------------+------+---------+-------+\n",
      "|00169217-cca6-4b4...|  King Terror V|Fathom|     NULL|      1|\n",
      "|00169217-cca6-4b4...|      King Sope|Fathom|     NULL|      1|\n",
      "|00169217-cca6-4b4...|       mcnaeric|Fathom|     NULL|      0|\n",
      "|00169217-cca6-4b4...|    EXTREMENOVA|Fathom|     NULL|      0|\n",
      "|00169217-cca6-4b4...| Psych0ticCamel|Fathom|     NULL|      0|\n",
      "|00169217-cca6-4b4...|Trap Lord David|Fathom|     NULL|      0|\n",
      "|00169217-cca6-4b4...|       DJ RAHHH|Fathom|     NULL|      1|\n",
      "|003218a6-bc5f-427...|       EcZachly|  Eden|     NULL|      1|\n",
      "|003218a6-bc5f-427...|     Casle cake|  Eden|     NULL|      0|\n",
      "|003218a6-bc5f-427...|   AnThRaXELiTE|  Eden|     NULL|      1|\n",
      "|003218a6-bc5f-427...|      Bolder OG|  Eden|     NULL|      1|\n",
      "|003218a6-bc5f-427...|    ILLICIT 117|  Eden|     NULL|      1|\n",
      "|003218a6-bc5f-427...| ChelseaFC26811|  Eden|     NULL|      0|\n",
      "|003218a6-bc5f-427...|  Oculus Impact|  Eden|     NULL|      0|\n",
      "|003218a6-bc5f-427...|  ostridge king|  Eden|     NULL|      0|\n",
      "|0043bc13-3751-4ca...|       Majinky9|Alpine|     NULL|      1|\n",
      "|0043bc13-3751-4ca...| Abyss Watchers|Alpine|     NULL|      1|\n",
      "|0043bc13-3751-4ca...|    sagrav12345|Alpine|     NULL|      1|\n",
      "|0043bc13-3751-4ca...|   INFAMOUS49ER|Alpine|     NULL|      1|\n",
      "|0043bc13-3751-4ca...|       RAKK9595|Alpine|     NULL|      1|\n",
      "+--------------------+---------------+------+---------+-------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "\"\"\"\n",
    "Task4 | medals_matches_players\n",
    " -> a row for every medal type a player gets in a match\n",
    "\n",
    "\"\"\"\n",
    "\n",
    "from pyspark.sql.functions import broadcast\n",
    "\n",
    "maps = spark.read.option(\"header\", \"true\").csv(\n",
    "    \"/home/iceberg/data/maps.csv\"\n",
    ")\n",
    "\n",
    "spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")\n",
    "\n",
    "\n",
    "# First, join the two large performance and match DataFrames.\n",
    "# We'll use the common 'match_id' column.\n",
    "combined_df = player_performances.join(matches_df, \"match_id\")\n",
    "# combined_df.show(2)\n",
    "\n",
    "# Now, join the result with the smaller 'maps' DataFrame.\n",
    "# We explicitly broadcast the 'maps' table for efficiency.\n",
    "medal_matches_players_df = combined_df.join(broadcast(maps), \"mapid\")\n",
    "\n",
    "# Show the result of the combined and broadcasted DataFrames.\n",
    "medal_matches_players_df.select(\"match_id\", \"player_gamertag\", \"name\", \"game_mode\", \"did_win\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+------+--------------------+\n",
      "|               mapid|            match_id|player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|is_team_game|         playlist_id|     game_variant_id|is_match_over|     completion_date|match_duration|game_mode|map_variant_id|  name|         description|\n",
      "+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+------+--------------------+\n",
      "|cc040aa1-f206-11e...|00169217-cca6-4b4...|  King Terror V|                   68|          68|          1597155| 1601153|                1|                       7|        2036|                                0|               17|               1|                      7|       2039|                               0|              17|                  1|          false|      PT29.9643588S|                14|                    11|         530.6970062255859|                       22|                       1|                       45|                          0|                              0|                               0|                          0|                               0|                              0|                  7|                   2|                         0|      1|      1|        true|2323b76a-db98-4e0...|257a305e-4dd3-41f...|         true|2016-03-13 00:00:...|          NULL|     NULL|          NULL|Fathom|The UNSC explores...|\n",
      "|cc040aa1-f206-11e...|00169217-cca6-4b4...|      King Sope|                  145|         145|          7925209| 7926307|                1|                       7|        2126|                                0|               10|               1|                      7|       2129|                               0|              10|                  4|          false|      PT35.7082005S|                11|                    10|         586.3655242919922|                       25|                       1|        33.33333206176758|                          0|                              0|                               0|                          0|                               0|                              0|                  5|                   4|                         0|      1|      1|        true|2323b76a-db98-4e0...|257a305e-4dd3-41f...|         true|2016-03-13 00:00:...|          NULL|     NULL|          NULL|Fathom|The UNSC explores...|\n",
      "+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------+------+--------------------+\n",
      "only showing top 2 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "\n",
    "\"\"\"\n",
    "Task5 | medals_matches_players\n",
    " Build a Spark job that\n",
    "Disabled automatic broadcast join with spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")\n",
    "Explicitly broadcast JOINs medals and maps\n",
    "Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets\n",
    "\n",
    "\"\"\"\n",
    "\n",
    "spark.sql(\"CREATE DATABASE IF NOT EXISTS halo_db\")\n",
    "spark.sql(\"USE halo_db\")\n",
    "\n",
    "# Set the number of buckets\n",
    "num_buckets = 16\n",
    "\n",
    "# display(match_details_df)\n",
    "# match_details_df.show(2)\n",
    "# Write each DataFrame as a bucketed table, partitioned by match_id\n",
    "(match_details_df.write\n",
    "  .bucketBy(num_buckets, \"match_id\")\n",
    "  # .sortBy(\"match_id\")\n",
    "  .mode(\"overwrite\")\n",
    "  .saveAsTable(\"match_details_bucketed\"))\n",
    "\n",
    "(matches_df.write\n",
    "  .bucketBy(num_buckets, \"match_id\")\n",
    "  # .sortBy(\"match_id\")\n",
    "  .mode(\"overwrite\")\n",
    "  .saveAsTable(\"matches_bucketed\"))\n",
    "\n",
    "# Assuming a DataFrame linking medals, matches, and players\n",
    "(medal_matches_players_df.write\n",
    "  .bucketBy(num_buckets, \"match_id\")\n",
    "  # .sortBy(\"match_id\")\n",
    "  .mode(\"overwrite\")\n",
    "  .saveAsTable(\"medal_matches_players_bucketed\"))\n",
    "\n",
    "\n",
    "# Read the bucketed tables\n",
    "details_b = spark.table(\"match_details_bucketed\")\n",
    "matches_b = spark.table(\"matches_bucketed\")\n",
    "medals_players_b = spark.table(\"medal_matches_players_bucketed\")\n",
    "\n",
    "# Join the tables. Spark will avoid a shuffle here.\n",
    "joined_df = (details_b\n",
    "    .join(matches_b, \"match_id\")\n",
    "    .join(medals_players_b, [\"match_id\", \"player_gamertag\"])\n",
    "    .select(\n",
    "        details_b.match_id,\n",
    "        details_b.player_gamertag,\n",
    "        details_b.player_total_kills,\n",
    "        medals_players_b.name,\n",
    "        matches_b.game_mode,\n",
    "        matches_b.playlist_id,\n",
    "        matches_b.mapid,\n",
    "        medals_players_b.did_win\n",
    "        \n",
    "    ))\n",
    "\n",
    "\n",
    "medal_matches_players_df.show(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------------+---------+\n",
      "|player_gamertag|avg_kills|\n",
      "+---------------+---------+\n",
      "|   gimpinator14|    109.0|\n",
      "|  I Johann117 I|     96.0|\n",
      "|BudgetLegendary|     83.0|\n",
      "|      GsFurreal|     75.0|\n",
      "|   Sexy is Back|     73.0|\n",
      "+---------------+---------+\n",
      "only showing top 5 rows\n",
      "\n",
      "+--------------------+----------+\n",
      "|         playlist_id|play_count|\n",
      "+--------------------+----------+\n",
      "|f72e0ef0-7c4a-430...|      7657|\n",
      "|2323b76a-db98-4e0...|      3174|\n",
      "|892189e9-d712-4bd...|      1974|\n",
      "|c98949ae-60a8-43d...|      1828|\n",
      "|f27a65eb-2d11-496...|       682|\n",
      "+--------------------+----------+\n",
      "only showing top 5 rows\n",
      "\n",
      "+--------------------+----------+\n",
      "|               mapid|play_count|\n",
      "+--------------------+----------+\n",
      "|c7edbf0f-f206-11e...|      7049|\n",
      "|c74c9d0f-f206-11e...|      1387|\n",
      "|cdb934b0-f206-11e...|      1351|\n",
      "|cb914b9e-f206-11e...|      1028|\n",
      "|ce1dc2de-f206-11e...|       979|\n",
      "+--------------------+----------+\n",
      "only showing top 5 rows\n",
      "\n",
      "+-----+-------------------+\n",
      "|mapid|killing_spree_count|\n",
      "+-----+-------------------+\n",
      "+-----+-------------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/07/22 21:13:23 WARN DataSourceV2Strategy: Can't translate true to source filter, unsupported expression\n"
     ]
    }
   ],
   "source": [
    "\"\"\"\n",
    "Aggregate the joined data frame to figure out questions like:\n",
    "Which player averages the most kills per game?\n",
    "Which playlist gets played the most?\n",
    "Which map gets played the most?\n",
    "Which map do players get the most Killing Spree medals on?\n",
    "\n",
    "\"\"\"\n",
    "from pyspark.sql.functions import avg\n",
    "\n",
    "\n",
    "#Which player averages the most kills per game?\n",
    "(joined_df.groupBy(\"player_gamertag\")\n",
    "    .agg(avg(\"player_total_kills\").alias(\"avg_kills\"))\n",
    "    .sort(desc(\"avg_kills\"))\n",
    "    .show(5)\n",
    "    )\n",
    "\n",
    "# Which playlist gets played the most?\n",
    "(joined_df.select(\"match_id\", \"playlist_id\").distinct() # Count each match once\n",
    "    .groupBy(\"playlist_id\")\n",
    "    .agg(count(\"*\").alias(\"play_count\"))\n",
    "    .sort(desc(\"play_count\"))\n",
    "    .show(5)\n",
    "    )\n",
    "\n",
    "\n",
    "# Which map gets played the most?\n",
    "(joined_df.select(\"match_id\", \"mapid\").distinct() # Count each match once\n",
    "    .groupBy(\"mapid\")\n",
    "    .agg(count(\"*\").alias(\"play_count\"))\n",
    "    .sort(desc(\"play_count\"))\n",
    "    .show(5)\n",
    "    )\n",
    "\n",
    "\n",
    "\n",
    "# This fixes the syntax, but the join is still unnecessary\n",
    "killing_spree_df = (joined_df\n",
    "    # Use a list for the join key to avoid ambiguous columns\n",
    "    .join(medals, [\"name\"]) \n",
    "    .filter(col(\"name\") == \"Killing Spree\")\n",
    "    # Group by the mapid, not the medal name\n",
    "    .groupBy(\"mapid\") \n",
    "    .agg(count(\"*\").alias(\"killing_spree_count\"))\n",
    "    .sort(desc(\"killing_spree_count\"))\n",
    "    .show(5)\n",
    "    )\n",
    "\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.16"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
