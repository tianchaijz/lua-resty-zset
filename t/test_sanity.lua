package.path = package.path .. ";lib/?.lua;"


local zset = require "resty.zset"


math.randomseed(os.time())


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
local sl = zs:sl()


do
    local x = sl:head_node()
    assert(not x)

    x = sl:tail_node()
    assert(not x)
end


while true do
    local score = random_choose(all)
    if not score then
        break
    end
    local value = "s" .. score
    zs:insert(score * math.random() * total, value)
    -- update to right score
    zs:insert(score, value)
end
assert(zs:len() == total)


do
    local x = sl:head_node()
    assert(x.value == "s1")
    assert(x.score == 1)

    assert(not zset.prev_node(x))

    x = zset.next_node(x)
    assert(x.value == "s2")
    assert(x.score == 2)

    x = sl:tail_node()
    assert(x.value == "s" .. total)
    assert(x.score == total)

    assert(not zset.next_node(x))

    x = zset.prev_node(x)
    assert(x.value == "s" .. (total - 1))
    assert(x.score == total - 1)


    print("iter first 10 nodes:")
    x = sl:head_node()
    for i = 1, 10 do
        print(string.format("i=%d value=%s score=%f", i, x.value, x.score))
        x = zset.next_node(x)
    end

    print("iter last 10 nodes:")
    x = sl:tail_node()
    for i = 1, 10 do
        print(string.format("i=%d value=%s score=%f", i, x.value, x.score))
        x = zset.prev_node(x)
    end
end


assert(total == zs:len())


print("rank 28:", zs:get_rank("s28"))
print("at rank 28:", zs:at_rank(28))


local function print_value(v, score)
    print(string.format("(%s, %f)", v, score))
end


print("rank [1, 10]:")
zs:iterate_range_by_rank(1, 10, print_value)


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
