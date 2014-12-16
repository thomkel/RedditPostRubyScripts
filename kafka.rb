require 'poseidon'

@producer = Poseidon::Producer.new(["localhost:9092"], "reddit_posts")

messages = []
messages << Poseidon::MessageToSend.new("post_submissions", "value1")
messages << Poseidon::MessageToSend.new("topic2", "value2")
@producer.send_messages(messages)