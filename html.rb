gem 'json'
gem 'nokogiri', '~> 1.6.5'
gem 'rest-client', '~> 1.7.2'

require 'open-uri'
require 'json'
require 'rest-client'
require 'nokogiri'


RestClient.post("http://karmadecay.com/index", :url => "www.reddit.com/2opn0m") do |response, request, result, &block|
  if [301, 302, 307].include? response.code
    @redirect_url = response.headers[:location]
  else
    @response = response.return!(request, result, &block)
  end
end

response = Nokogiri::HTML(RestClient.get("http://karmadecay.com/#{@redirect_url}"))		

page_info = response.css('div#wr').css('div#content').css('table.search')
results = page_info.css('tr.result')
result = results[0]

info = result.css('td.info')
num = result.css('td.no').text.gsub(/[^\d]/,'').to_i

href = ""
title = info.css('div.title a')

if title.empty? 
	title = info.css('div.titleR a')
end

if !title.empty?
	href = title[0]['href']
end

puts "title: " + title.to_s
puts "href: " + href.to_s


