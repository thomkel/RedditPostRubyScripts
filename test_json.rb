gem 'json'
require 'json'

reddit_info = {"1" => {"str" => "id"}, "2" => {"i32" => 12345},
	"3" => {"str" => "something"}, "4" => {"str" => "else"},
	"5" => {"i32" => 9845}, "6" => {"str" => "is"},
	"7" => {"i32" => 1234}, "8" => {"str" => "wrong"},
	"9" => {"i32" => 1234}, "10" => {"i32" => 1234}, 
	"11" => {"i32" => 1234}, "12" => {"i32" => 1234}, 
	"13" => {"i32" => 1234}
}

json = JSON.generate(reddit_info)

read_json = JSON.parse(json)
read_json["2"]["i32"] = 948474

puts read_json["2"]["i32"]
