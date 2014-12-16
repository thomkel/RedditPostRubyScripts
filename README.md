RedditPostRubyScripts
=====================
Ruby script to scrape Reddit post/repost data and send to Kafka queue

get_posts.rb server 4 primary functions:

1.	Pulls data on latest posts from Reddit by hitting www.reddit.com/new.json
2.	Checks karmadecay.com to see if post is repost. If new repost information is found, gets historical information from Reddit
3.	Queries Cassandra to see if image_id for post/repost is on record
4.	Sends new data to Kafka queue
