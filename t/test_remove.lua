package.path = package.path .. ";lib/?.lua;"


local zset = require "resty.zset"


local total = 100
local all = {}
for i = 1, total do
    all[#all + 1] = i
end


local function random_choose(t)
    if #t == 0 then
        return
    end
    local i = math.random(#t)
    return table.remove(t, i)
end


local zs = zset.new()

while true do
    local score = random_choose(all)
    if not score then
        break
    end
    local value = "s" .. score
    zs:insert(score, value)
end


assert(total == zs:len())


zs:remove_gte(90)
assert(zs:len() == 89)
assert(zs:tail() == "s89")


zs:remove_gt(80)
assert(zs:len() == 80)
assert(zs:tail() == "s80")


zs:remove_lte(10)
assert(zs:len() == 70)
assert(zs:head() == "s11")


zs:remove_lt(20)
assert(zs:len() == 61)
assert(zs:head() == "s20")


zs:remove_range(20, false, 21, false)
assert(zs:len() == 59)
assert(zs:head() == "s22")


zs:remove_range(22, true, 23, false)
assert(zs:len() == 58)
assert(zs:head() == "s22")
assert(zs:at_rank(2) == "s24")


-- remove nothing
zs:remove_range(22, true, 23, true)
assert(zs:len() == 58)
assert(zs:head() == "s22")
assert(zs:at_rank(2) == "s24")


zs:remove_range(22, false, 23, true)
assert(zs:len() == 57)
assert(zs:head() == "s24")


zs:remove_range(24, false, 30, false)
assert(zs:len() == 50)
assert(zs:head() == "s31")


zs:limit_front(30)
assert(zs:len() == 30)
assert(zs:head() == "s31")
assert(zs:tail() == "s60")


zs:limit_back(10)
assert(zs:len() == 10)
assert(zs:head() == "s51")
assert(zs:tail() == "s60")


zs:pop_head()
assert(zs:head() == "s52")


zs:pop_tail()
assert(zs:tail() == "s59")


print("OK!")
