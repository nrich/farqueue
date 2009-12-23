#!/usr/bin/lua 

module('Farqueue', package.seeall)

local http = require('socket.http')
local json = require('json')
local posix = require('posix')
local lash = require('lash')

function New(queue, construct)
    construct = construct or {}

    local host = construct.Host or '127.0.0.1'
    local port = construct.Port or 9094

    local url = string.format('http://%s:%s/%s', host, port, queue) 

    local exit_on_result = false

    local function dequeue(self, callback, looplimit)
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
		else
		    if exit_on_result then
			exit_on_result = false
			return
		    end
		end
            elseif status == 404 then
		if looplimit then
		    if looplimit == 0 then
			return
		    end

		    looplimit = looplimit - 1
		end

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

    local function message(self, data, timeout)
	timeout = timeout or 1

	local id = lash.MD5.string2hex(posix.getpid().ppid .. os.time())

	local message = {
	    id = id,
	    data = data,
	} 

	self:enqueue(message)

	local save_url = url

	local result

	url = url .. id

	exit_on_result = true
	self:dequeue(function(d)
	    result = d
	end, timeout)

	url = save_url

	return result
    end

    local function subscribe(self, callback)
	self:dequeue(function(message)
	    local data = message.data
	    local id = message.id

	    local res = callback(data)

	    local save_url = url
	    url = url .. id
	    self:enqueue(res)
	    url = save_url
	end) 
    end

    local farqueue = {
        enqueue = enqueue,
        dequeue = dequeue,
        message = message,
	subscribe = subscribe,
    }

    return farqueue
end

