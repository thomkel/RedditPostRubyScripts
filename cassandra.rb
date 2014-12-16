gem 'json'
gem 'cassandra-driver', '~> 1.0.0'
gem 'nokogiri', '~> 1.6.5'
gem 'rest-client', '~> 1.7.2'

require 'open-uri'
require 'json'
require 'rest-client'
require 'nokogiri'
require 'cassandra'



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

		if title.empty? 
			title = info.css('div.titleR a')
		end

		if !title.empty?
			href = 	title[0]['href']
			puts "Successfully found href: " + href.to_s

			pages = href.split("/")
			subreddit = pages[4]
			reddit_id = pages[6]
			title = title[0].text
			image_id = @reddit_ids[reddit_id]

			submitted = info.css('div.submitted a')

			user = ""
			if submitted.empty?
				puts "Could not get user info " + info.to_s
			else
				user = submitted[0].text
			end

			score = info.css('div div.votes b.no').text.gsub(/[^\d]/,'').to_i
			comments = info.css('div div.comments b.no').text.gsub(/[^\d]/,'').to_i

			new_image_id = @reddit_ids[reddit_id.to_s]
			puts "NEW IMAGE ID FOR " + reddit_id.to_s + " : " + new_image_id.to_s

			if new_image_id != -1

				puts "FOUND MATCH!"

			end

			reddit_data = [reddit_id, nil, title, subreddit, comments, user,
				nil, nil, nil, score, nil, nil, nil
			]

			# karma_decay_posts.push(generate_json_info_for_post(reddit_data))

			if (image_id == -1) & (new_image_id != -1)
				puts "FOUND REPOST ALREADY STORED IN CASSANDRA: " + new_image_id.to_s
				image_id = new_image_id
			end
		else
			puts "Error: title not found for " + info.to_s
		end
	end		

	# image_id = process_karma_posts(karma_decay_posts, image_id)

	# return image_id

end

@reddit_ids = nil
@current_id = 0

cluster = Cassandra.cluster
keyspace = 'reddit_posts'
session = cluster.connect(keyspace)

urls = Hash.new(-1)
max_id = 0

session.execute("SELECT * FROM reposts").each do |row|
	id = row['image_id'].to_i
	reddit_id = row['reddit_id'].to_s
	urls[reddit_id] = id

	puts "CASSANDRA: ID: " + reddit_id + ", IMAGE: " + id.to_s

	if id > max_id
		max_id = id
	end
end

@reddit_ids = urls
@current_id = max_id

reddit_info = {"1" => {"str" => "2oqt2t"}, "2" => {"i32" => 12345},
	"3" => {"str" => "something"}, "4" => {"str" => "else"},
	"5" => {"i32" => 9845}, "6" => {"str" => "is"},
	"7" => {"i32" => 1234}, "8" => {"str" => "wrong"},
	"9" => {"i32" => 1234}, "10" => {"i32" => 1234}, 
	"11" => {"i32" => 1234}, "12" => {"i32" => 1234}, 
	"13" => {"i32" => 1234}
}

reddit_json = JSON.generate(reddit_info)

post_json = JSON.parse(reddit_json)

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
