#!/usr/bin/lua 

module('Farqueue', package.seeall)

local http = require('socket.http')
local json = require('json')
local posix = require('posix')

function New(queue, construct)
    construct = construct or {}

    local host = construct.Host or '127.0.0.1'
    local port = construct.Port or 9094

    local url = string.format('http://%s:%s/%s', host, port, queue) 

    local function dequeue(self, callback)
        while true do
            local body, status = http.request(url)
	    local struct = json.decode(body)

            if status == 200 then
                local ok, err = pcall(callback, struct)

		if not ok then
		    if construct.Requeue then
			-- re-enqueue the request
			self:enqueue(struct)
		    end

		    -- rethrow the error 
		    error(err)
		end
            elseif status == 404 then
                -- queue empty, back off
                posix.sleep(1)
            else
                 error('Fatal error" ' .. status)
            end 
        end
    end

    local function enqueue(self, data)
        --local post = string.format('data=%s', json.encode(data))
        local post = json.encode(data)

        local req,status = http.request({
            url = url,
            method = 'POST',
            headers = {
                ["Content-Length"] = string.len(post),
                ["Content-Type"] =  "application/x-www-form-urlencoded",
            },
            source = ltn12.source.string(post),
        })

        -- TODO check status of return from enqueue
    end 

    local farqueue = {
        enqueue = enqueue,
        dequeue = dequeue,
    }

    return farqueue
end

