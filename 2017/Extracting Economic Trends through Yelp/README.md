This class project attempted to extract frequency information from the 2017 Yelp dataset in an attempt to visualize economic growth and decline. Limitations within the Yelp dataset prevented any true conclusions from being found, but the methods used in this project strongly parallel what could be done in future projects. (Note that there was a second part of this project involving collaborative filtering and index-searching to determine recommendations for Yelp users. These files have not been included here, although they are mentioned in both the final paper and the presentation.)

VVV~~FROM THE PROJECT'S README~~VVV

To create the CSV files needed for analysis, complete the following steps (note you will need many different modules installed. Use Anaconda for the easiest time.)

Run yelp2csv.py after changing the path to your own. Comment out the JSON files you don't want to convert to CSVs.
This will create a CSV equivalent for each of the JSON files you mark to do.
Run city_state_division.py after changing the path to your own. Comment out the relevant sections if you want to do just cities or states.
This will create a pickled dictionary (cities_and_businesses.pkl) that'll contain the data for which states contain which businesses.
This will create two directories: one for States and one for Cities, that'll be filled with pickled pandas DataFrames for each state/city chosen.
Run IsolateUsers.py after changing the path to your own. Comment out the relevant sections if you want to do just cities or states.
This will create two directories: a Users directory for Cities and a Users directory for States, where user-counts will be placed.
This will create pickled pandas DataFrames that represent the number of new yelp reviewers who've commented in a given city/state per day.
For Task 2 of our final project, we wanted to see if evidence could be found of the financial recession of 2008 (or any financial fluctuations) based solely on Yelp data.

There are four hypotheses we used in attempting to answer this question:

There will be a larger-than-average number of reviews in times of economic prosperity (and the opposite in economic hardship).
When people have more money to spend, they are more likely to go out, and reviews should reflect this.
There will be an increased mention of terms like "cheap" and "generous" or "expensive" and "measly" during economic hardship.
The amount of a good provided will be viewed more extremely when there is less money to spend on goods.
There will be an increased number of negative reviews during times of economic hardship.
When people haven't the money to spend, they may be more critical when shopping or eating out, as their money is seen as more valuable.
There will be a higher rate of visits to lower-end, cheaper businesses than higher-end ones during economic hardship.
When less money is available, consumers will choose cheaper-end businesses to compensate.
Moreover, we believe that cities will be hit differently by the shifting economy, so these effects should be more noticeable in cities harder hit than those less hit. It is under these assumptions that our research and analyses have been made.

There are separate Jupyter notebooks for each of these hypotheses. Feel free to take a look and add to them as you'd like.