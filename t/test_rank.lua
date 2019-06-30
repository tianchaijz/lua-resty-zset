package.path = package.path .. ";lib/?.lua;"


local math = require "math"
local zset = require "resty.zset"


local random = math.random


math.randomseed(require("os").time())


local int32_max = 2 ^ 32 - 1
local total = 1000000
local zs = zset.new(total)


local function rand()
    return random(1, int32_max)
end


local function insert()
    local mem = {}
    local sorted = {}

    for _ = 1, total do
        local n = rand()
        if not mem[n] then
            mem[n] = true
            zs:insert(n, n)
            sorted[#sorted + 1] = n
        end
    end

    table.sort(sorted)

    return sorted
end


local function delete(len)
    for _ = 1, len do
        zs:pop_head()
    end
end


for _ = 1, 10 do
    local sorted = insert()
    local len = #sorted
    for i = 1, len do
        local v, score = zs:at_rank(i)
        assert(v == score)
        assert(v == sorted[i])
        assert(zs:get_rank(sorted[i]) == i)
    end

    delete(len)
    assert(zs:len() == 0, zs:len())
end


print("OK!")
