** SOCCER ANALYSIS - TEAM AND PLAYER LEVEL**

** TECHNOLOGIES USED  **
1. Scrapy
2. Mongo
3. Spark
4. Plotly

** FILES INFORMATION **

scrapy.py -  crawls the data from website sofifa.com

Home_away_adv - gives the performance of 2 teams at home and away and player composition of two teams 

(Run As - spark-submit scrapy.py)

team_attack_defensive - gives the attacking and defensive work rate

(Run As - spark-submit team_attack_defensive.py 'Real Madrid' 'FC Barcelona')

outside_inside_box - gives the goals scored from various playes for each league

(Run As - spark-submit outside_inside_box.py)

wage_py - Machine Learing model prediction for a new player who could be replacement of current aging player

(Run As - spark-submit wage_py)

aggresiveness_in_teams - aggressiveness in teams in each league

(Run As - spark-submit aggressiveness_in_teams.py)

average_goals_scored - average goals scored by each league over the years

(Run As - spark-submit average_goals_scored.py)


correlation_features - relating the players features like pace, stamina, agility

(Run As - spark-submit correlation_features.py)

player_replacement - suggests a replacement a given player for a particular position
(Run As - spark-submit player_replacement.py 'B Matuidi')

** CAN RUN ALL THE PROGRAMS ON THE SPARK INSTALLED MACHINE **



