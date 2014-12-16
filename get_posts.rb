gem 'cassandra-driver', '~> 1.0.0'
gem 'nokogiri', '~> 1.6.5'
gem 'rest-client', '~> 1.7.2'
gem 'poseidon', '~> 0.0.5'

require 'poseidon'
require 'open-uri'
require 'json'
require 'rest-client'
require 'nokogiri'
require 'cassandra'

# purpose: 	1. get reddit submissions. 
# 			2. check karma decay to see if repost
# 			3. parse  new reddit submissions and found karma decay posts and submit to kafka queue.

def run_initialize
	# current_id is the latest image_id to be used
	# reddit_ids is a hash table to check if a post found through karma decay is already in Cassandra and has an image_id
	@current_id, @reddit_ids = get_persisted_urls 
	@count = 0
	@after = nil

	@producer = Poseidon::Producer.new(["localhost:9092"], "reddit_posts")
	@posts_queue = [] 

	get_posts  # fill posts_queue with 200 posts

	puts "got posts"

	process_posts_info
end

def process_posts_info
	while(true)
		thr1 = Thread.new { check_karma_decay_for_reposts }
		thr2 = Thread.new { check_karma_decay_for_reposts }
		thr3 = Thread.new { check_karma_decay_for_reposts }

		threads = [thr1, thr2, thr3]

		if @posts_queue.size < 50
			thr4 = Thread.new { get_posts }
			thread.push[thr4]
		end

		threads.each {|t| t.join}
	end
end

def get_posts
	while @posts_queue.size < 200
		url = "https://www.reddit.com/new.json?limit=100&count=#{@count}&after=#{@after}"
		json = open(url).read
		json_data = JSON.parse(json)
		posts = json_data["data"]["children"]
		@after = json_data["data"]["after"]
		@count = @count + 100

		reposts = get_post_info(posts)

		reposts.each do |repost|
			@posts_queue.push(repost)
		end
	end
end

def get_post_info(posts)
	reddit_data = []

	posts.each do |post|
		data = post["data"]
		url = data["url"]

		if url.include?('imgur')

			reddit_info = [data["id"], nil, data["title"], data["subreddit"], data["num_comments"],
				data["author"], data["created"], Time.at(data["created"]), data["created_utc"],
				data["score"], data["ups"] + data["downs"], data["ups"], data["downs"]
			]

			reddit_json = generate_json_info_for_post(reddit_info)

			reddit_data.push(reddit_json)	
		end
	end		

	return reddit_data	
end

def check_karma_decay_for_reposts
	post = @posts_queue.shift

	post_json = JSON.parse(post)

	reddit_id = post_json["1"]["str"]
	image_url = "www.reddit.com/#{reddit_id}"
	puts "reddit_id: " + reddit_id.to_s
	@response = get_karma_decay_page(image_url)
	page_info = @response.css('div#wr').css('div#content').css('table.search')
	image_id = nil
	# if no matches, there is a ".ns" class
	check_if_no_matches = page_info.css("tr.ns")

	if check_if_no_matches.empty?  # AKA found matches
		results = page_info.css('tr.result')
		prev_num = 0

		image_id = get_karma_decay_info(results)
	else
		@current_id = @current_id + 1
		image_id = @current_id
	end

	post_json["2"]["i32"] = image_id

	puts "Processed reddit post"
	send_to_kafka(post_json)

end	

def get_karma_decay_info(results)
	image_id = -1
	karma_decay_posts = []
	prev_num = 0

	results.each do |result|
		info = result.css('td.info')
		num = result.css('td.no').text.gsub(/[^\d]/,'').to_i

		if !(num > prev_num)
			break
		end

		prev_num = num

		title = info.css('div.title a')

		if !title.empty?
			href = 	title[0]['href']
			puts "Successfully found href: " + href.to_s

			pages = href.split("/")
			subreddit = pages[4]
			reddit_id = pages[6]
			title = title[0].text
			image_id = @reddit_ids[reddit_id]

			# updating
			url = "https://www.reddit.com/#{reddit_id}.json"
			got_json = false

			while(!got_json)
				RestClient.get(url) do |response, request, result, &block|
				  if [301, 302, 307].include? response.code
				    url = response.headers[:location]
				    puts "GOT REDIRECT URL: " + url
				  else
				    @response = response.return!(request, result, &block)
				    got_json = true
				  end
				end
			end

			json = open(url).read
			json_data = JSON.parse(json)
			data = json_data[0]["data"]
			posts = data["children"]

			reposts = get_post_info(posts)

			new_image_id = @reddit_ids[reddit_id.to_s]

			if new_image_id != -1

				puts "FOUND MATCH!"

			end

			if (image_id == -1) & (new_image_id != -1)
				puts "FOUND REPOST ALREADY STORED IN CASSANDRA: " + new_image_id.to_s
				image_id = new_image_id
			else

				karma_decay_posts.push(reposts.first)	
			end

		end
	end		

	image_id = process_karma_posts(karma_decay_posts, image_id)

	return image_id

end

def process_karma_posts(posts, image_id)
	id = image_id

	if (image_id == -1) & (posts.size > 0)
		@current_id = @current_id + 1
		id = @current_id
	end

	posts.each do |post|
		post_json = JSON.parse(post)
		post_json["2"]["i32"] = id
		puts "Processed Karma Post"
		send_to_kafka(post_json)
	end

	return id
end

def get_karma_decay_page(image_url)
	RestClient.post("http://karmadecay.com/index", :url => image_url) do |response, request, result, &block|
	  if [301, 302, 307].include? response.code
	    @redirect_url = response.headers[:location]
	  else
	    @response = response.return!(request, result, &block)
	  end
	end

	return Nokogiri::HTML(RestClient.get("http://karmadecay.com/#{@redirect_url}"))		
end

def send_to_kafka(post)
	# eventually, send to kafka
	# for testing, using puts

	json_post = JSON.generate(post)
	messages = []
	messages << Poseidon::MessageToSend.new("post-submissions", json_post)
	@producer.send_messages(messages)

	puts "Successful post: " + json_post.to_s
end

def generate_json_info_for_post(info)
	# json format
	# {"1":{"str":"t3_pj780"},"2":{"i32":123456},"3":{"str":"title"},"4":{"str":"subreddit"},"5":{"i32":30},"6":{"str":"redditor"},"7":{"i32":928384756},"8":{"str":"raw_time"},"9":{"i32":9283847},"10":{"i32":16},"11":{"i32":18},"12":{"i32":17},"13":{"i32":1}}	
	 # may want to change to search for file type substring
	reddit_info = {"1" => {"str" => info[0]}, "2" => {"i32" => info[1]},
		"3" => {"str" => info[2]}, "4" => {"str" => info[3]},
		"5" => {"i32" => info[4]}, "6" => {"str" => info[5]},
		"7" => {"i32" => info[6].to_i}, "8" => {"str" => info[7]},
		"9" => {"i32" => info[8].to_i}, "10" => {"i32" => info[9]}, 
		"11" => {"i32" => info[10]}, "12" => {"i32" => info[11]}, 
		"13" => {"i32" => info[12]}
	}

	return JSON.generate(reddit_info)
end

def get_persisted_urls
	# from cassandra, need highest image_id (so we can increment)
	# AND list of image_ids and urls

	cluster = Cassandra.cluster
	keyspace = 'reddit'
	# keyspace = 'reddit_posts'
	session = cluster.connect(keyspace)

	urls = Hash.new(-1)
	max_id = 0

	session.execute("SELECT * FROM repost_info ALLOW FILTERING").each do |row|
	# session.execute("SELECT * FROM reposts ALLOW FILTERING").each do |row|

		id = row['image_id'].to_i
		urls[row['reddit_id'].to_s] = id

		if id > max_id
			max_id = id
		end
	end

	return max_id, urls

end

run_initialize
