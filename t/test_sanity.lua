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
    local value = "a" .. score
    zs:insert(score, value)
end


assert(total == zs:len())


print("rank 28:", zs:get_rank("a28"))
print("at rank 28:", zs:at_rank(28))


local function print_value(v, score)
    print(string.format("(%s, %f)", v, score))
end


print("rank [1, 10]:")
zs:iterate_range_by_rank(1, 10, function(v)
    print(v)
end)


print("reverse rank [1, 10]:")
zs:iterate_range_by_rank(zs:reverse_rank(1), zs:reverse_rank(10), print_value)


print("score [1, 10]:")
zs:iterate_range_by_score(1, false, 10, false, print_value)


print("score (1, 10]:")
zs:iterate_range_by_score(1, true, 10, false, print_value)


print("score (1, 10):")
zs:iterate_range_by_score(1, true, 10, true, print_value)


print("------------------ dump ------------------")
zs:dump()


print("------------------ dump after limit front 10 ------------------")
zs:limit_front(10)
zs:dump()


print("------------------ dump after limit back 5 ------------------")
zs:limit_back(5)
zs:dump()


print("------------------ dump after remove range [8, 9] ------------------")
zs:remove_range(8, false, 9, false)
zs:dump()


print("------------------ dump after remove range (6, 10) ------------------")
zs:remove_range(6, true, 10, true)
zs:dump()


print("pop head: ", zs:pop_head())
print("pop tail: ", zs:pop_tail())


print("------------------ dump empty ------------------")
zs:dump()


print("OK!")
