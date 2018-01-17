require 'set'
require 'csv'
require 'redis'
require 'sidekiq'
AUTHORS = Hash[file = File.read("datasets/TheRedPill/unique_authors.csv").split("\n").collect{|x| x.split(" ").reverse}];false
$redis = Redis.new
class ExtractTimelines
  include Sidekiq::Worker
  sidekiq_options queue: :blackpilling
  def perform(filename, folder)
    file = File.open("/media/dgaff/main/blackpill_paper/datasets/#{folder}_timeline/#{filename.split("/").last.gsub(".bz2", ".json")}", "w")
    comments = []
    stream("bzip2 -dck "+filename) do |line|
      comment = JSON.parse(line)
      if !comment.nil? && comment["author"] != "[deleted]" && $redis.sismember("blackpilling_#{folder}", comment["author"])
        comments << comment
        file.write([comment["created_utc"], comment["subreddit"], comment["author"], comment["id"], comment["parent_id"], comment["link_id"], comment["score"], comment["gilded"], comment["body"]].to_json+"\n")
      end
    end;false
    file.close
  end

  def self.kickoff(folder="TheRedPill")
    $redis.sadd("blackpilling_#{folder}", AUTHORS.keys)
    `ls /media/dgaff/main/data/reddit_data/comments/*/*.bz2`.split("\n").each do |filename|
      self.perform_async(filename, folder)
    end
  end

  def stream(command)
    IO.popen(command) do |io|
      while (line = io.gets) do
        yield line
      end
    end  
  end
end
