-- Copyright (C) Jinzheng Zhang (tianchaijz).
-- Ported from Redis skiplist C implementation.


local math = require "math"

local setmetatable = setmetatable
local random = math.random
local table_new


do
    local ok
    ok, table_new = pcall(require, "table.new")
    if not ok then
        table_new = function(narr, nrec) return {} end
    end
end


local _skiplist_max_level = 32
local _skiplist_p = 0.25


local _M = {}
local _mt = { __index = _M }


local function lt_in(lhs, rhs, inclusive)
    if inclusive then
        return lhs <= rhs
    end

    return lhs < rhs
end


local function lt_ex(lhs, rhs, exclusive)
    if exclusive then
        return lhs < rhs
    end

    return lhs <= rhs
end


-- higher levels are less likely to be returned.
local function random_level()
    local level = 1
    while random() < _skiplist_p do
        level = level + 1
    end

    return level <= _skiplist_max_level and level or _skiplist_max_level
end


--[[
    struct skiplist_node_level_s {
        skiplist_node_t         *forward;
        int                      span;
    };

    struct skiplist_node_s {
        void                    *obj;
        double                   score;
        skiplist_node_t         *backward;
        skiplist_node_level_t   *level;
    };

    struct skiplist_s {
        skiplist_node_t         *header;
        skiplist_node_t         *tail;
        int                      level;
        int                      length;
    };
]]


local function new_node(level, score, value)
    local lvl = table_new(level, 0)
    for i = 1, level do
        lvl[i] = { forward = nil, span = 0 }
    end

    local node = {
        value = value,
        score = score,
        backward = nil,
        level = lvl,
    }

    return node
end


function _M.new()
    local sl = {
        header = new_node(_skiplist_max_level, 0, nil),
        tail = nil,
        level = 1,
        length = 0,
    }

    local self = {
        _sl = sl,
        _rank = table_new(_skiplist_max_level, 0),
        _update = table_new(_skiplist_max_level, 0),
    }

    return setmetatable(self, _mt)
end


function _M.len(self)
    return self._sl.length
end


local function insert(self, score, value)
    local sl = self._sl
    local rank = self._rank
    local update = self._update

    local x = sl.header
    local l = sl.level
    for i = l, 1, -1 do
        -- store rank that is crossed to reach the insert position
        rank[i] = i == l and 0 or rank[i + 1]
        local y = x.level[i]
        local z = y.forward
        while z and (z.score < score or
                (z.score == score and z.value < value)) do
            rank[i] = rank[i] + y.span
            x = z
            y = x.level[i]
            z = y.forward
        end
        update[i] = x
    end

    local level = random_level()
    if level > sl.level then
        for i = sl.level + 1, level do
            rank[i] = 0
            update[i] = sl.header
            update[i].level[i].span = sl.length
        end
        sl.level = level
    end

    x = new_node(level, score, value)

    for i = 1, level do
        local y = x.level[i]
        local z = update[i].level[i]

        y.forward = z.forward
        z.forward = x

        -- update span covered by update[i] as x is inserted here
        local span = rank[1] - rank[i]
        y.span = z.span - span
        z.span = span + 1
    end

    -- increment span for untouched levels
    for i = level + 1, sl.level do
        local y = update[i].level[i]
        y.span = y.span + 1
    end

    if update[1] == sl.header then
        x.backward = nil
    else
        x.backward = update[1]
    end

    local y = x.level[1].forward
    if y then
        y.backward = x
    else
        sl.tail = x
    end

    sl.length = sl.length + 1

    return x
end
_M.insert = insert


local function delete_node(sl, x, update)
    for i = 1, sl.level do
        local y = x.level[i]
        local z = update[i].level[i]
        if z.forward == x then
            z.span = z.span + y.span - 1
            z.forward = y.forward
        else
            z.span = z.span - 1
        end
    end

    local y = x.level[1]
    if y.forward then
        y.forward.backward = x.backward
    else
        sl.tail = x.backward
    end

    local lvl = sl.header.level
    while sl.level > 1 and not lvl[sl.level].forward do
        lvl[sl.level].span = 0
        sl.level = sl.level - 1
    end

    sl.length = sl.length - 1
end


function _M.update(self, curscore, value, newscore)
    local sl = self._sl
    local update = self._update

    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i].forward
        while y and (y.score < curscore or
                (y.score == curscore and y.value < value)) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    -- jump to our object: note that this function assumes that the
    -- object with the matching score exists
    x = x.level[1].forward

    if (not x.backward or x.backward.score < newscore) and
            (not x.level[1].forward or
                    x.level[1].forward.score > newscore) then
        x.score = newscore
        return x
    end

    -- no way to resuse the old one
    delete_node(sl, x, update)

    return insert(self, newscore, value)
end


local function delete(self, score, value)
    local sl = self._sl
    local update = self._update

    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i].forward
        while y and (y.score < score or
                (y.score == score and y.value < value)) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    x = x.level[1].forward
    if x and x.value == value then
        delete_node(sl, x, update)
        return true
    end

    return false
end
_M.delete = delete


-- delete [s, e], note that s and e need to be 1-based.
function _M.delete_range_by_rank(self, s, e, cb)
    local sl = self._sl
    local update = self._update

    if s > sl.length or e < 1 then
        return 0
    end

    local removed = 0
    local traversed = 0

    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i]
        while y.forward and (traversed + y.span < s) do
            traversed = traversed + y.span
            x = y.forward
            y = x.level[i]
        end
        update[i] = x
    end

    traversed = traversed + 1
    x = x.level[1].forward
    while x and traversed <= e do
        local y = x.level[1].forward

        cb(x)
        delete_node(sl, x, update)

        removed = removed + 1
        traversed = traversed + 1

        x = y
    end

    return removed
end


function _M.delete_range_by_score(self, minscore, minex, maxscore, maxex, cb)
    local sl = self._sl
    local update = self._update

    local removed = 0

    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i].forward
        while y and lt_in(y.score, minscore, minex) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    x = x.level[1].forward
    while x and lt_ex(x.score, maxscore, maxex) do
        local y = x.level[1].forward

        cb(x)
        delete_node(sl, x, update)

        removed = removed + 1

        x = y
    end

    return removed
end


function _M.head(self)
    local x = self._sl.header

    x = x.level[1].forward
    if x then
        return x.value, x.score
    end
end


function _M.tail(self)
    local x = self._sl.tail
    if x then
        return x.value, x.score
    end
end


function _M.pop_head(self)
    local x = self._sl.header

    x = x.level[1].forward
    if x then
        local v = x.value
        local score = x.score
        delete(self, score, v)
        return v, score
    end
end


function _M.pop_tail(self)
    local x = self._sl.tail
    if x then
        local v = x.value
        local score = x.score
        delete(self, score, v)
        return v, score
    end
end


function _M.get_node_by_rank(self, rank)
    local sl = self._sl

    if rank < 1 or rank > sl.length then
        return
    end

    local traversed = 0
    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i]
        while y.forward and (traversed + y.span <= rank) do
            traversed = traversed + y.span
            x = y.forward
            y = x.level[i]
        end

        if traversed == rank then
            return x
        end
    end
end


function _M.get_rank(self, score, value)
    local sl = self._sl

    local rank = 0
    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i]
        local z = y.forward
        while z and (z.score < score or
                (z.score == score and z.value <= value)) do
            rank = rank + y.span
            x = z
            y = x.level[i]
            z = y.forward
        end

        if x.value == value then
            return rank
        end
    end

    return 0
end


-- return the 0-based rank of given score.
function _M.get_score_rank(self, score, ex)
    local sl = self._sl

    local rank = 0
    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i]
        local z = y.forward
        while z and lt_ex(z.score, score, ex) do
            rank = rank + y.span
            x = z
            y = x.level[i]
            z = y.forward
        end
    end

    return rank
end


local function is_in_range(self, minscore, minex, maxscore, maxex)
    if minscore > maxscore or (minscore == maxscore and (minex or maxex)) then
        return false
    end

    local sl = self._sl
    local x = sl.tail
    if not x or lt_in(x.score, minscore, minex) then
        return false
    end

    x = sl.header.level[1].forward
    if not x or lt_in(maxscore, x.score, maxex) then
        return false
    end

    return true
end
_M.is_in_range = is_in_range


function _M.first_in_range(self, minscore, minex, maxscore, maxex)
    if not is_in_range(self, minscore, minex, maxscore, maxex) then
        return
    end

    local sl = self._sl
    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i].forward
        while y and lt_in(y.score, minscore, minex) do
            x = y
            y = x.level[i].forward
        end
    end

    -- this is an inner range, so the next node cannot be nil
    x = x.level[1].forward
    if lt_ex(x.score, maxscore, maxex) then
        return x
    end
end


function _M.last_in_range(self, minscore, minex, maxscore, maxex)
    if not is_in_range(self, minscore, minex, maxscore, maxex) then
        return
    end

    local sl = self._sl
    local x = sl.header
    for i = sl.level, 1, -1 do
        local y = x.level[i].forward
        while y and lt_ex(y.score, maxscore, maxex) do
            x = y
            y = x.level[i].forward
        end
    end

    if lt_ex(minscore, x.score, minex) then
        return x
    end
end


function _M.iterate(self, cb)
    local rank = 0
    local x = self._sl.header

    x = x.level[1].forward
    while x do
        rank = rank + 1
        cb(x, rank)
        x = x.level[1].forward
    end
end


_M.lt_in = lt_in
_M.table_new = table_new


return _M
