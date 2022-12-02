package.path = package.path .. ";lib/?.lua;"

local cjson = require "cjson.safe"
local zset = require "resty.zset"


local function equal(a, b)
    if type(a) ~= type(b) then return false end
    if type(a) == "table" then return cjson.encode(a) == cjson.encode(b) end
    return a == b
end


local function get_range_by_rank(zs, s, e)
    local t = {}
    local cb = function(v) t[#t + 1] = v end
    zs:iterate_range_by_rank(s, e, cb)
    return t
end


local function get_range_by_score(zs, s, e)
    local t = {}
    local cb = function(v) t[#t + 1] = v end
    zs:iterate_range_by_score(s, false, e, false, cb)
    return t
end


local zs = zset.new()
assert(zs:len() == 0)


zs:insert(100, "a")
zs:insert(101, "b")
zs:insert(102, "c")


assert(zs:len() == 3)
assert(zs:get_rank("a") == 1)
assert(zs:get_rank("b") == 2)
assert(zs:get_rank("c") == 3)
assert(zs._sl:get_score_rank(3) == 0)
assert(zs._sl:get_score_rank(102, true) == 2)
assert(zs._sl:get_score_rank(102, false) == 3)
assert(zs._sl:get_score_rank(1000) == 3)
assert(equal(get_range_by_rank(zs, 1, 1), { "a" }))
assert(equal(get_range_by_rank(zs, 1, 2), { "a", "b" }))
assert(equal(get_range_by_rank(zs, 1, 3), { "a", "b", "c" }))
assert(equal(get_range_by_rank(zs, 1, 4), { "a", "b", "c" }))
assert(equal(get_range_by_rank(zs, 3, 1), { "c", "b", "a" }))
assert(equal(get_range_by_rank(zs, 3, 2), { "c", "b" }))


zs = zset.new(500000)
assert(zs:len() == 0)


local total = 500000
for i = 1, total do
    -- test update
    zs:insert(i * math.random() * total, tostring(i))
    -- update to right score
    zs:insert(i, tostring(i))
end
assert(zs:len() == total)


do
    local v, score = zs:head()
    assert(v == "1")
    assert(score == 1)

    v, score = zs:tail()
    assert(v == tostring(total))
    assert(score == total)
end


for _ = 1, total do
    local n = math.random(total)
    assert(zs:at_rank(n) == tostring(n))
    assert(zs:get_rank(tostring(n)) == n)
end


local r1, r2 = 100, 100000
local t1 = get_range_by_rank(zs, r1, r2)
local t2 = get_range_by_rank(zs, r2, r1)
assert(#t1 == #t2)
assert(#t1, r2 - r1 + 1)
for i, value in ipairs(t1) do
    assert(value == t2[#t2 -i + 1], value)
end


local function test_range(zs, s, e, func)
    local t = func(zs, s, e)
    assert(#t > 0)
    for i in ipairs(t) do
        if s < e then
            assert(t[i] == tostring(s + (i - 1)))
        else
            assert(t[i] == tostring(s - (i - 1)))
        end
    end
end


for _ = 1, 10 do
    local s = math.random(total)
    local e = math.random(total)
    test_range(zs, s, e, get_range_by_rank)
    test_range(zs, s, e, get_range_by_score)
end


for i = 1, total do
    zs:delete(tostring(i))
end
assert(zs:len() == 0)


for i = 1, 100000 do
    zs:insert(-i, tostring(i))
end
assert(zs:len() == 100000, zs:len())


print("OK!")
