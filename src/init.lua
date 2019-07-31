local log = require('log')
local json = require('json')
local digest = require('digest')
local fiber = require('fiber')

---@param orig table
---@return table
local function deepcopy(orig)
    local copy
    if orig ~= nil and type(orig) == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[deepcopy(orig_key)] = deepcopy(orig_value)
        end
        setmetatable(copy, deepcopy(getmetatable(orig)))
    else
        copy = orig
    end
    return copy
end

--- @class EtcdClient
local EtcdClient = {
    __zero_key = '\0',
    _get_prefix = function(key)
        local end_range = key

        end_range = { string.byte(key, 1, -1) }

        local len = #key
        local i = len
        while i > 0 do
            if end_range[i] < 0xff then
                end_range[i] = end_range[i] + 1
                while #end_range > i do
                    table.remove(end_range)
                end

                return string.char(unpack(end_range))
            end
            i = i - 1
        end
        return '\0'
    end,

    _decode = function(obj, key)

        if obj ~= nil and type(obj) == 'table' and obj[key] ~= nil then
            obj[key] = digest.base64_decode(obj[key])
        end


    end,

    _encode = function(obj, key)

        if obj ~= nil and type(obj) == 'table' and obj[key] ~= nil then
            obj[key] = digest.base64_encode(obj[key])
        end


    end,
    --- @private
    _try_request = function(self, path, request_object, opts)


        if opts == nil then
            opts = {}
        end

        opts.timeout = opts.timeout or self.default_timeout

        local req_res

        local deadline = fiber.time() + opts.timeout

        local body = json.encode(request_object)

        repeat
            --TODO retry only  if etcd server is not responding
            for _, etcd_server in ipairs(self.addresses) do
                req_res = self.client:request('POST', etcd_server .. self.path_prefix .. path, body, self.req_opts)

                if req_res.status == 200 then
                    return json.decode(req_res.body)
                end
            end

            fiber.sleep(0.1)

        until fiber.time() > deadline

        return
    end,


    --- @param self EtcdClient
    _prepare_compare = function(self, comparison)
        self._encode(comparison, 'key')
        self._encode(comparison, 'value')
        self._encode(comparison, 'range_end')
    end,

    --- @param self EtcdClient
    _prepare_request_op = function(self, request_op)

        if request_op.request_range ~= nil then
            self._prepare_range_request(self, request_op.request_range)
        end

        if request_op.request_put ~= nil then
            self._prepare_put_request(self, request_op.request_put)
        end

        if request_op.request_delete_range ~= nil then
            self._prepare_delete_range_request(self, request_op.request_delete_range)
        end

        if request_op.request_txn ~= nil then
            self._prepare_txn_request(self, request_op.request_txn)
        end


    end,

    --- @param self EtcdClient
    _prepare_range_request = function(self, range_request)

        self._encode(range_request, 'key')
        self._encode(range_request, 'range_end')

    end,

    --- @param self EtcdClient
    _prepare_put_request = function(self, put_request)

        self._encode(put_request, 'key')
        self._encode(put_request, 'value')

    end,

    --- @param self EtcdClient
    _prepare_delete_range_request = function(self, delete_range_request)
        self._encode(delete_range_request, 'key')
        self._encode(delete_range_request, 'value')

    end,

    --- @param self EtcdClient
    _prepare_txn_request = function(self, txn_request)
        if txn_request.compare ~= nil then
            for _, comparison in ipairs(txn_request.compare) do
                self._prepare_compare(self, comparison)
            end
        end

        if txn_request.success ~= nil then
            for _, success in ipairs(txn_request.success) do
                self._prepare_request_op(self, success)
            end
        end

        if txn_request.failure ~= nil then
            for _, failure in ipairs(txn_request.failure) do
                self._prepare_request_op(self, failure)
            end
        end

    end,

    --- @param self EtcdClient
    _prepare_range_response = function(self, range_response)
        if range_response.kvs ~= nil then
            for _, key_obj in ipairs(range_response.kvs) do
                self._decode(key_obj, 'key')
                self._decode(key_obj, 'value')
            end
        end

        if range_response.count == nil then
            range_response.count = 0
        end
        if range_response.kvs == nil then
            range_response.kvs = {}
        end

    end,

    --- @param self EtcdClient
    _prepare_put_response = function(self, put_response)
        if put_response.prev_kv ~= nil then
            self._decode(put_response.prev_kv, 'key')
            self._decode(put_response.prev_kv, 'value')
        end
    end,

    --- @param self EtcdClient
    _prepare_delete_range_response = function(self, delete_range_response)
        if delete_range_response.prev_kvs ~= nil then
            for _, key_obj in ipairs(delete_range_response.kvs) do
                self._decode(key_obj, 'key')
                self._decode(key_obj, 'value')
            end
        end
    end,

    --- @param self EtcdClient
    _prepare_txn_response = function(self, txn_response)
        if txn_response.responses ~= nil then
            for _, resp in ipairs(txn_response.responses) do
                resp = self._prepare_response_op(self, resp)
            end
        end

        if txn_response.succeeded == nil then
            txn_response.succeeded = false
        end

    end,

    _prepare_response_op = function(self, txn_response)

        local resp = txn_response
        if resp.response_range ~= nil then
            self._prepare_range_response(self, resp.response_range)
        end
        if resp.response_put ~= nil then
            self._prepare_put_response(self, resp.response_put)
        end
        if resp.response_delete_range ~= nil then
            self._prepare_delete_range_response(self, resp.response_delete_range)
        end
        if resp.response_txn ~= nil then
            self._prepare_txn_response(self, resp.response_txn)
        end

    end
}

EtcdClient.__index = EtcdClient
setmetatable(EtcdClient, {
    ---@return EtcdClient
    __call = function(_, addresses)

        local http_client = require('http.client').new()

        local obj = {
            addresses = addresses,
            client = http_client,
            default_timeout = 2,
            path_prefix = '/v3beta', --TODO update when stable released
            req_opts = {
                timeout = 0.5,
                headers = { ['Content-type'] = 'application/json' }
            }
        }

        local self = setmetatable(obj, EtcdClient)

        log.info({ 'EtcdClient created', obj })
        return self
    end,
})

---@param ttl number
---@param id number
---@return table | nil
function EtcdClient:lease_grant(ttl, id)
    local req_data = {
        TTL = ttl,
        ID = id
    }
    return self._try_request(self, '/lease/grant', req_data)
end

---@param id number
---@return table | nil
function EtcdClient:lease_keepalive(id)
    local req_data = {
        ID = id
    }

    return self._try_request(self, '/lease/keepalive', req_data)
end

---@param id number
---@return table | nil
function EtcdClient:lease_revoke(id)
    local req_data = {
        ID = id
    }

    return self._try_request(self, '/lease/revoke', req_data)
end

function EtcdClient:kv_put(key, value, lease, prev_kv, ignore_value, ignore_lease)


    key = digest.base64_encode(key)
    value = digest.base64_encode(value)

    local req_data = {
        key = key,
        value = value,
        lease = lease,
        prev_kv = prev_kv,
        ignore_value = ignore_value,
        ignore_lease = ignore_lease,
    }

    return self._try_request(self, '/kv/put', req_data)

end

function EtcdClient:kv_range(key, range_end, limit, revision,
                             sort_order, sort_target, serializable,
                             keys_only, count_only,
                             min_mod_revision, max_mod_revision, min_create_revision, max_create_revision)

    key = digest.base64_encode(key)
    if range_end ~= nil then
        range_end = digest.base64_encode(range_end)
    end

    local req_data = {
        key = key,
        range_end = range_end,
        limit = limit,
        revision = revision,
        sort_order = sort_order,
        sort_target = sort_target,
        serializable = serializable,
        keys_only = keys_only,
        count_only = count_only,
        min_mod_revision = min_mod_revision,
        max_mod_revision = max_mod_revision,
        min_create_revision = min_create_revision,
        max_create_revision = max_create_revision
    }

    local res = self._try_request(self, '/kv/range', req_data)

    if res == nil then
        return
    end

    if res.kvs == nil then
        res.kvs = {}
    end
    for _, key_obj in ipairs(res.kvs) do
        key_obj.key = digest.base64_decode(key_obj.key)
        if key_obj.value ~= nil then
            key_obj.value = digest.base64_decode(key_obj.value)
        end

    end
    return res
end


--- https://github.com/etcd-io/etcd/blob/master/etcdserver/etcdserverpb/rpc.proto

function EtcdClient:kv_txn(compare_ar, success_ar, failure_ar)

    compare_ar = deepcopy(compare_ar)
    success_ar = deepcopy(success_ar)
    failure_ar = deepcopy(failure_ar)

    local txn_request = {
        compare = compare_ar,
        success = success_ar,
        failure = failure_ar,
    }

    self._prepare_txn_request(self, txn_request)

    local txn_response = self._try_request(self, '/kv/txn', txn_request)

    if txn_response == nil then
        return
    end

    self._prepare_txn_response(self, txn_response)
    return txn_response

end

return EtcdClient