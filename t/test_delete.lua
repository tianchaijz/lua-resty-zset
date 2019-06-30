package.path = package.path .. ";lib/?.lua;"


local math = require "math"
local zset = require "resty.zset"


local random = math.random


math.randomseed(require("os").time())


local int32_max = 2 ^ 32 - 1
local total = 1000000
local mem = {}
local zs = zset.new(total)


local function insert()
    for i = 1, total do
        mem[i] = random(1, int32_max)
        zs:insert(i, mem[i])
    end
end


local function delete()
    for i = 1, total do
        zs:delete(mem[i])
    end
end


for _ = 1, 10 do
    insert()
    delete()
    assert(zs:len() == 0)
end


print("OK!")
