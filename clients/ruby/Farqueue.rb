#!/usr/bin/ruby

require 'net/http'
require 'uri'
require 'json'

class Farqueue
    def initialize(queue, host = '127.0.0.1', port = 9094)
	@queue = queue
        @host = host
        @port = port
    end

    def enqueue(data = {})
        http = Net::HTTP.new(@host, @port)

        headers = {
            'Content-Type' => 'application/x-www-form-urlencoded'
        }

        res, data = http.post("/#{@queue}", JSON.JSON(data), headers)

        return JSON.parse(res.body)
    end

    def dequeue(callback)
	while true do
	    http = Net::HTTP.new(@host, @port)

	    res, data = http.get("/#{@queue}")

	    if res.code == '200' then
		callback(JSON.parse(data))
	    elsif res.code == '404' then
		sleep(1)
	    else
		puts(res.code)
	    end
	end
    end
end

