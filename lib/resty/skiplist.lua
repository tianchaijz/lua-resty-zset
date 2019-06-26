-- Copyright (C) Jinzheng Zhang (tianchaijz).
-- Ported from Redis skiplist C implementation.


local ffi = require "ffi"
local math = require "math"

local ffi_gc = ffi.gc
local ffi_new = ffi.new
local ffi_fill = ffi.fill
local ffi_cast = ffi.cast
local ffi_sizeof = ffi.sizeof

local tonumber = tonumber
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


ffi.cdef[[
typedef struct skiplist_node_level_s skiplist_node_level_t;
typedef struct skiplist_node_s skiplist_node_t;
typedef struct skiplist_s skiplist_t;


struct skiplist_node_level_s {
    skiplist_node_t         *forward;
    int                      span;
};

struct skiplist_node_s {
    int                      ref;
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


local NULL = ffi_new("void*", nil)
local int_arr_t = ffi.typeof("int[?]")
local uintptr_t = ffi.typeof("uintptr_t")
local skiplist_t = ffi.typeof("skiplist_t")
local skiplist_node_t = ffi.typeof("skiplist_node_t[1]")
local skiplist_node_ptr_t = ffi.typeof("skiplist_node_t*")
local skiplist_node_ptr_arr_t = ffi.typeof("skiplist_node_t*[?]")
local skiplist_node_level_arr_t = ffi.typeof("skiplist_node_level_t[?]")

local _skiplist_max_level = 32
local _skiplist_p = 0.25
local _skiplist_header_level_size =
    ffi_sizeof(skiplist_node_level_arr_t, _skiplist_max_level)


local _registry = {}
local _ref_free = 0
local _ref_nil = -1


local _M = {}
local _mt = { __index = _M }


local function doref(obj)
    if obj == nil then
        return _ref_nil
    end

    local ref = _registry[_ref_free]
    if ref and ref ~= 0 then
        _registry[_ref_free] = _registry[ref]
    else
        ref = #_registry + 1
    end

    _registry[ref] = obj

    return ref
end


local function unref(ref)
    -- print("unref: ", ref)
    if ref > 0 then
        _registry[ref] = _registry[_ref_free]
        _registry[_ref_free] = ref
    end
end


local function ptr2num(ptr)
    return tonumber(ffi_cast(uintptr_t, ptr))
end


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


local function new_node(level)
    local node = ffi_new(skiplist_node_t)
    local lvl = ffi_new(skiplist_node_level_arr_t, level)

    node[0].ref = doref({ node, lvl })
    node[0].level = lvl

    return ffi_new(skiplist_node_ptr_t, node)
end


local function free_skiplist(sl)
    -- print("free skiplist header: ", sl.header.ref)
    unref(sl.header.ref)
end


function _M.new(size)
    local sl = ffi_gc(ffi_new(skiplist_t), free_skiplist)

    sl.header = new_node(_skiplist_max_level)
    sl.level = 1

    local self = {
        _sl = sl,
        _dict = table_new(0, size or 1),
        _rank = ffi_new(int_arr_t, _skiplist_max_level),
        _update = ffi_new(skiplist_node_ptr_arr_t, _skiplist_max_level),
    }

    return setmetatable(self, _mt)
end


function _M.len(self)
    return self._sl.length
end


function _M.clear(self)
    local sl = self._sl

    local header = sl.header

    header.score = 0
    header.backward = NULL
    ffi_fill(header.level, _skiplist_header_level_size, 0)

    sl.tail = NULL
    sl.level = 1
    sl.length = 0
end


function _M.node2value(self, node)
    return self._dict[ptr2num(node)]
end


local function insert(self, score, value)
    local sl = self._sl
    local dict = self._dict
    local rank = self._rank
    local update = self._update

    local x = sl.header
    local l = sl.level - 1
    for i = l, 0, -1 do
        -- store rank that is crossed to reach the insert position
        rank[i] = i == l and 0 or rank[i + 1]
        local y = x.level[i]
        local z = y.forward
        while z ~= NULL and (z.score < score or
                (z.score == score and dict[ptr2num(z)] < value)) do
            rank[i] = rank[i] + y.span
            x = z
            y = x.level[i]
            z = y.forward
        end
        update[i] = x
    end

    local level = random_level()
    if level > sl.level then
        for i = sl.level, level - 1 do
            rank[i] = 0
            update[i] = sl.header
            update[i].level[i].span = sl.length
        end
        sl.level = level
    end

    x = new_node(level)
    x.score = score
    dict[ptr2num(x)] = value

    for i = 0, level - 1 do
        local y = x.level[i]
        local z = update[i].level[i]

        y.forward = z.forward
        z.forward = x

        -- update span covered by update[i] as x is inserted here
        local span = rank[0] - rank[i]
        y.span = z.span - span
        z.span = span + 1
    end

    -- increment span for untouched levels
    for i = level, sl.level - 1 do
        local y = update[i].level[i]
        y.span = y.span + 1
    end

    if update[0] == sl.header then
        x.backward = NULL
    else
        x.backward = update[0]
    end

    local y = x.level[0].forward
    if y == NULL then
        sl.tail = x
    else
        y.backward = x
    end

    sl.length = sl.length + 1

    return x
end
_M.insert = insert


local function delete_node(self, sl, x, update)
    for i = 0, sl.level - 1 do
        local y = x.level[i]
        local z = update[i].level[i]
        if z.forward == x then
            z.span = z.span + y.span - 1
            z.forward = y.forward
        else
            z.span = z.span - 1
        end
    end

    local y = x.level[0]
    if y.forward == NULL then
        sl.tail = x.backward
    else
        y.forward.backward = x.backward
    end

    local lvl = sl.header.level
    while sl.level > 1 and lvl[sl.level - 1].forward == NULL do
        sl.level = sl.level - 1
    end

    sl.length = sl.length - 1

    -- free
    self._dict[ptr2num(x)] = nil
    unref(x.ref)
end


function _M.update(self, curscore, value, newscore)
    local sl = self._sl
    local dict = self._dict
    local update = self._update

    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i].forward
        while y ~= NULL and (y.score < curscore or
                (y.score == curscore and dict[ptr2num(y)] < value)) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    -- jump to our object: note that this function assumes that the
    -- object with the matching score exists
    x = x.level[0].forward

    if (x.backward == NULL or x.backward.score < newscore) and
            (x.level[0].forward == NULL or
                    x.level[0].forward.score > newscore) then
        x.score = newscore
        return x
    end

    -- no way to resuse the old one
    delete_node(self, sl, x, update)

    return insert(self, newscore, value)
end


local function delete(self, score, value)
    local sl = self._sl
    local dict = self._dict
    local update = self._update

    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i].forward
        while y ~= NULL and (y.score < score or
                (y.score == score and dict[ptr2num(y)] < value)) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    x = x.level[0].forward
    if x ~= NULL and dict[ptr2num(x)] == value then
        delete_node(self, sl, x, update)
        return true
    end

    return false
end
_M.delete = delete


-- delete [from, to], note that from and to need to be 1-based.
function _M.delete_range_by_rank(self, from, to, cb)
    local sl = self._sl
    local dict = self._dict
    local update = self._update

    if from > sl.length or to < 1 then
        return 0
    end

    local removed = 0
    local traversed = 0

    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i]
        while y.forward ~= NULL and (traversed + y.span < from) do
            traversed = traversed + y.span
            x = y.forward
            y = x.level[i]
        end
        update[i] = x
    end

    traversed = traversed + 1
    x = x.level[0].forward
    while x ~= NULL and traversed <= to do
        local y = x.level[0].forward
        local idx = ptr2num(x)
        local value = dict[idx]

        cb(value, x.score)

        delete_node(self, sl, x, update)
        removed = removed + 1
        traversed = traversed + 1

        x = y
    end

    return removed
end


function _M.delete_range_by_score(self, minscore, minex, maxscore, maxex, cb)
    local sl = self._sl
    local dict = self._dict
    local update = self._update

    local removed = 0

    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i].forward
        while y ~= NULL and lt_in(y.score, minscore, minex) do
            x = y
            y = x.level[i].forward
        end
        update[i] = x
    end

    x = x.level[0].forward
    while x ~= NULL and lt_ex(x.score, maxscore, maxex) do
        local y = x.level[0].forward
        local idx = ptr2num(x)
        local value = dict[idx]

        cb(value, x.score)

        delete_node(self, sl, x, update)
        removed = removed + 1

        x = y
    end

    return removed
end


function _M.pop_head(self)
    local x = self._sl.header

    x = x.level[0].forward
    if x ~= NULL then
        local v = self._dict[ptr2num(x)]
        local score = x.score
        delete(self, score, v)
        return v, score
    end
end


function _M.pop_tail(self)
    local x = self._sl.tail
    if x ~= NULL then
        local v = self._dict[ptr2num(x)]
        local score = x.score
        delete(self, score, v)
        return v, score
    end
end


local function get_node_by_rank(self, rank)
    local sl = self._sl

    if rank < 1 or rank > sl.length then
        return
    end

    local traversed = 0
    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i]
        while y.forward ~= NULL and (traversed + y.span <= rank) do
            traversed = traversed + y.span
            x = y.forward
            y = x.level[i]
        end

        if traversed == rank then
            return x
        end
    end
end
_M.get_node_by_rank = get_node_by_rank


function _M.at_rank(self, rank)
    local x = get_node_by_rank(self, rank)
    if x ~= NULL then
        return self._dict[ptr2num(x)], x.score
    end
end


function _M.get_rank(self, score, value)
    local sl = self._sl
    local dict = self._dict

    local rank = 0
    local x = sl.header
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i]
        local z = y.forward
        while z ~= NULL and (z.score < score or
                (z.score == score and dict[ptr2num(z)] <= value)) do
            rank = rank + y.span
            x = z
            y = x.level[i]
            z = y.forward
        end

        if dict[ptr2num(x)] == value then
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
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i]
        local z = y.forward
        while z ~= NULL and lt_ex(z.score, score, ex) do
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
    if x == NULL or lt_in(x.score, minscore, minex) then
        return false
    end

    x = sl.header.level[0].forward
    if x == NULL or lt_in(maxscore, x.score, maxex) then
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
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i].forward
        while y ~= NULL and lt_in(y.score, minscore, minex) do
            x = y
            y = x.level[i].forward
        end
    end

    -- this is an inner range, so the next node cannot be NULL
    x = x.level[0].forward
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
    for i = sl.level - 1, 0, -1 do
        local y = x.level[i].forward
        while y ~= NULL and lt_ex(y.score, maxscore, maxex) do
            x = y
            y = x.level[i].forward
        end
    end

    if lt_ex(minscore, x.score, minex) then
        return x
    end
end


function _M.iterate(self, cb)
    local dict = self._dict

    local rank = 0
    local x = self._sl.header

    x = x.level[0].forward
    while x ~= NULL do
        rank = rank + 1
        cb(dict[ptr2num(x)], x.score, rank)
        x = x.level[0].forward
    end
end


_M.lt_in = lt_in
_M.table_new = table_new


return _M
