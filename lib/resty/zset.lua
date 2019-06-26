-- Copyright (C) Jinzheng Zhang (tianchaijz).


local skiplist = require "resty.skiplist"


local print = print
local tostring = tostring
local setmetatable = setmetatable
local string_format = string.format

local lt_in = skiplist.lt_in
local table_new = skiplist.table_new


local _M = {}
local _mt = { __index = _M }


function _M.new(size)
    local zset = {
        _sl = skiplist.new(size),
        _dict = table_new(0, size or 1),
    }

    setmetatable(zset, _mt)

    return zset
end


function _M.insert(self, score, value)
    local dict = self._dict
    local curscore = dict[value]
    if curscore then
        if curscore ~= score then
            self._sl:update(curscore, value, score)
            dict[value] = score
        end
        return
    end

    self._sl:insert(score, value)
    dict[value] = score
end


function _M.delete(self, value)
    local dict = self._dict
    local score = dict[value]
    if score then
        self._sl:delete(score, value)
        dict[value] = nil
        return true
    end
end


function _M.pop_head(self)
    local v, score = self._sl:pop_head()
    if v then
        self._dict[v] = nil
        return v, score
    end
end


function _M.pop_tail(self)
    local v, score = self._sl:pop_tail()
    if v then
        self._dict[v] = nil
        return v, score
    end
end


function _M.score(self, value)
    return self._dict[value]
end


function _M.len(self)
    return self._sl:len()
end


-- return value and score
function _M.at_rank(self, rank)
    return self._sl:at_rank(rank)
end


function _M.get_rank(self, value)
    local score = self._dict[value]
    if not score then
        return nil
    end

    return self._sl:get_rank(score, value)
end


function _M.reverse_rank(self, rank)
    return self._sl:len() - rank + 1
end


-- remove [from, to]
local function remove_helper(self, from, to, cb)
    local dict = self._dict
    local delete_cb = function(value, score)
        dict[value] = nil
        if cb then
            cb(value, score)
        end
    end

    return self._sl:delete_range_by_rank(from, to, delete_cb)
end


-- remove (-∞, score)
function _M.remove_lt(self, score, cb)
    local rank = self._sl:get_score_rank(score, true)
    if rank == 0 then
        return
    end

    return remove_helper(self, 1, rank, cb)
end


-- remove (-∞, score]
function _M.remove_lte(self, score, cb)
    local rank = self._sl:get_score_rank(score, false)
    if rank == 0 then
        return
    end

    return remove_helper(self, 1, rank, cb)
end


-- remove (score, +∞)
function _M.remove_gt(self, score, cb)
    local sl = self._sl
    local rank = sl:get_score_rank(score, false) + 1
    local len = sl:len()
    if rank > len then
        return
    end

    return remove_helper(self, rank, len, cb)
end


-- remove [score, +∞)
function _M.remove_gte(self, score, cb)
    local sl = self._sl
    local rank = sl:get_score_rank(score, true) + 1
    local len = sl:len()
    if rank > len then
        return
    end

    return remove_helper(self, rank, len, cb)
end


function _M.remove_range(self, minscore, minex, maxscore, maxex, cb)
    local dict = self._dict
    local delete_cb = function(value, score)
        dict[value] = nil
        if cb then
            cb(value, score)
        end
    end

    return self._sl:delete_range_by_score(minscore, minex, maxscore, maxex,
                                          delete_cb)
end


function _M.limit_front(self, n, cb)
    local len = self._sl:len()
    if n >= len then
        return 0
    end

    return remove_helper(self, n + 1, len, cb)
end


function _M.limit_back(self, n, cb)
    local len = self._sl:len()
    if n >= len then
        return 0
    end

    local to = len - n

    return remove_helper(self, 1, to, cb)
end


-- 1-based rank, inclusive
function _M.iterate_range_by_rank(self, from, to, cb)
    local sl = self._sl
    local len = sl:len()

    from = from or len
    to = to or len

    local reverse = from > to
    local span = reverse and (from - to + 1) or (to - from + 1)

    local x = sl:get_node_by_rank(from)
    local n = 0
    local value
    while x and n < span do
        n = n + 1
        value = sl:node2value(x)
        cb(value, x.score)
        if reverse then
            x = x.backward
        else
            x = x.level[0].forward
        end
    end
end


-- default inclusive
function _M.iterate_range_by_score(self, from, fromex, to, toex, cb)
    local sl = self._sl

    local x
    local reverse = from > to
    if reverse then
        x = sl:last_in_range(to, toex, from, fromex)
    else
        x = sl:first_in_range(from, fromex, to, toex)
    end

    local value
    while x do
        value = sl:node2value(x)
        cb(value, x.score)
        if reverse then
            x = x.backward
            if not x or lt_in(x.score, to, toex) then
                return
            end
        else
            x = x.level[0].forward
            if not x or lt_in(to, x.score, toex) then
                return
            end
        end
    end
end


local function print_node(value, score, rank)
    print(string_format("(%d, %f, %s)", rank, score, tostring(value)))
end


function _M.dump(self)
    self._sl:iterate(print_node)
end


return _M
