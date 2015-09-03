#!/usr/bin/env tarantool

local json = require('json')
local http = require('http.client')
local fiber = require('fiber')
local net_box = require('net.box')

local user = arg[1]
local flyd_tmp_dir = '/tmp/flyd'
local config = string.format([[
default_cfg = {
	pid_file   = "%s",
	wal_dir    = "%s",
	snap_dir   = "%s",
	sophia_dir = "%s",
	logger     = "%s",
}
instance_dir = "%s/instance"
]], flyd_tmp_dir, flyd_tmp_dir, flyd_tmp_dir,
    flyd_tmp_dir, flyd_tmp_dir, flyd_tmp_dir)

local flyd_config = string.format([[
return {
	box = {
		listen = 3311,
		log_level = 6, -- debug
		logger = '%s/flyd.log'
	},

	http_server = {
		host = '0.0.0.0',
		port = 8080
	},

	proxy_servers = {
		'127.0.0.1:12345',
	},
	callback_host = '127.0.0.1:8080',

	path_to_iata_codes = '%s/dictionary.lua',
	dep_delay_delta = 5,
	retry_timeout = 60,
}
]], flyd_tmp_dir, flyd_tmp_dir)

local function initialize_test()
	os.execute('rm -rf ' .. flyd_tmp_dir)
	os.execute(string.format('mkdir -p /home/%s/.config/tarantool', user))
	os.execute(string.format("mkdir -p %s/%s", flyd_tmp_dir, 'instance'))

	-- prepare config file (for tarantool)
	local f, err = io.open(string.format('/home/%s/.config/tarantool/tarantool', user), 'w')
	if not f then
		print(string.format("open failed: %s", err))
		os.exit(1)
	end

	local ok, msg = f:write(config)
	if not ok then
		print(string.format("write failed: %s", msg))
		os.exit(1)
	end
	f:close()

	-- prepare config file (for flyd)
	f, err = io.open(string.format('%s/flyd.conf', flyd_tmp_dir), 'w')
	if not f then
		print(string.format("open failed: %s", err))
		os.exit(1)
	end

	ok, msg = f:write(flyd_config)
	if not ok then
		print(string.format("write failed: %s", msg))
		exit(1)
	end
	f:close()

	-- install flyd
	os.execute(string.format("cp %s %s/instance/%s", 'src/flyd.lua', flyd_tmp_dir, 'flyd.lua'))
	os.execute(string.format("cp %s %s/%s", 'src/dictionary.lua', flyd_tmp_dir, 'dictionary.lua'))

	-- install httpd
	os.execute(string.format("cp %s %s/instance/%s", 'test/httpd.lua', flyd_tmp_dir, 'httpd.lua'))

	-- run flyd, httpd
	os.execute(string.format("tarantoolctl start flyd %s/flyd.conf", flyd_tmp_dir))
	os.execute(string.format("tarantoolctl start httpd", flyd_tmp_dir))
	os.execute("sleep 1") -- wait for daemons

	local conn = net_box:new('127.0.0.1', 3311)
	if not conn:ping() then
		print("ping failed, it seems that flyd is not started")
		os.exit(1)
	end

	return conn
end

local function deinitialize_test(conn)
	os.execute("tarantoolctl stop httpd")
	os.execute("tarantoolctl stop flyd")

	conn:close()
end

local function flyd_get(conn, t, loc)
	return conn:call('flyd_get_flight_info_json', loc, t)
end

local function flyd_put(conn, arg)
	return conn:call('flyd_new_flight', arg)
end

local function flyd_cb(action, args)
	return http.request('GET', string.format('http://127.0.0.1:12345/cb/%s/AA/100/2015/07/20?%s', action, args))
end

local function flyd_divert(args)
	return http.request('GET', string.format('http://127.0.0.1:12345/cb/div/ZR/100/2015/07/20?%s', args))
end

-------------------------------------------
-------------------------------------------

local conn = initialize_test()
local test = require('tap').test('flyd')

test:plan(2)

test:test('flyd.put.get', function(test)
	test:plan(3)

	test:test('flyd.put - arguments', function(test)
		test:plan(6)

		local ret = flyd_put(conn, {})
		test:isnil(ret[1][1], 'put: non string arg')

		ret = flyd_put(conn, '')
		test:isnil(ret[1][1], 'put: empty string arg')

		ret = flyd_put(conn, 'invalid_string')
		test:is(ret[1][1], false, 'put: invalid string arg')

		ret = flyd_put(conn, '1|2|3')
		test:is(ret[1][1], false, 'put: invlalid args count')

		ret = flyd_put(conn, 'a|b|c|d|e|a')
		test:is(ret[1][1], false, 'put: invlalid args')

		ret = flyd_put(conn, 'a|b|1|2|3|4')
		test:is(ret[1][1], true, 'put: vlalid args')
	end)

	test:test('flyd.get - arguments', function(test)
		test:plan(7)

		local ret = flyd_get(conn, 'non table', 'ru')
		test:istable(ret, 'ret is table')
		test:istable(ret[1], 'ret[1] is table')
		test:isnil(ret[1][1], 'ret[1][1] is nil')

		ret = flyd_get(conn, { '1|2|3|4|5|6' }, 'us')
		test:isnil(ret[1][1], 'invalid locale')

		ret = flyd_get(conn, { '1|2|3|4' }, 'en')
		test:is(ret[1][2], false, 'invalid args cnt')

		ret = flyd_get(conn, { '1|2|3|b|5|6' }, 'en')
		test:is(ret[1][2], false, 'invalid args')

		ret = flyd_get(conn, { '1|2|3|4|5|6' }, 'en')
		test:is(ret[1][2], 'null', 'not found')
	end)

	test:test('flyd.put.get', function(test)
		test:plan(15)

		flyd_put(conn, 'X|AA|100|20|7|2015')
		fiber.sleep(1) -- wait for `put' complete

		local ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		test:isnt(ret[1][2], 'null', 'insert + select success')

		local data = json.decode(ret[1][2])
		test:is(data.reservationId, 'CODE', 'check reservation code')
		test:is(data.reservationFor.provider.iataCode, 'AA', 'check provider iata')
		test:is(data.reservationFor.flightNumber, '100', 'check flight number')
		test:is(data.reservationFor.departureTime, '2015-07-20T00:00:00', 'check departure date')
		test:is(data.reservationFor.arrivalTime, '2015-07-21T00:00:00', 'check arrival date')

		flyd_put(conn, 'X|ERR|100|20|7|2015')
		fiber.sleep(1)

		ret = flyd_get(conn, { 'CODE|ERR|100|20|7|2015' }, 'en')
		test:is(ret[1][2], 'null', 'check for error select')

		flyd_put(conn, 'X|ZR|100|20|7|2015')
		fiber.sleep(1)

		ret = flyd_get(conn, { 'CODE|ZR|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:isnil(data.reservationFor.provider.name, 'check (unknown) provider iata')
		test:isnil(data.reservationFor.departureAirport.name, 'check departure airport (unknown)')
		test:isnil(data.reservationFor.arrivalAirport.name, 'check arrival airport (unknown)')

		flyd_put(conn, 'X|CB|100|20|7|2015')
		fiber.sleep(1)

		ret = flyd_get(conn, { 'CODE|CB|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationFor.departureTime, 'invalid date returned', 'check departure date')

		-- invalid json inside cb respnonse
		flyd_put(conn, 'X|ERR_JSON|100|20|7|2015')
		fiber.sleep(1)

		ret = flyd_get(conn, { 'CODE|ERR_JSON|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationId, 'CODE', 'check reservation code')
		test:is(data.reservationFor.provider.iataCode, 'ERR_JSON', 'check provider iata')

		-- `error' field inside cb response
		flyd_put(conn, 'X|ERR_IN_JSON|100|20|7|2015')
		fiber.sleep(1)

		ret = flyd_get(conn, { 'CODE|ERR_IN_JSON|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationId, 'CODE', 'check reservation code')
		test:is(data.reservationFor.provider.iataCode, 'ERR_IN_JSON', 'check provider iata')
	end)
end)

test:test('flyd.update', function(test)
	test:plan(8)

	local resp = http.request('GET', 'http://127.0.0.1:8080/invalid_key')
	test:is(resp.status, 405, 'check invalid cb method')

	resp = http.request('POST', 'http://127.0.0.1:8080/invalid_key', 'non json string')
	test:is(resp.status, 400, 'check invalid json in cb')

	resp = http.request('POST', 'http://127.0.0.1:8080/invalid_key', '{"value":true}')
	test:is(resp.status, 403, 'check invalid key for cb')

	test:test('flyd.update.departure_gate', function(test)
		test:plan(4)

		local resp = flyd_cb('dep_gate', 'fdgate=NG&fdterm=NT')
		local ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		local data = json.decode(ret[1][2])
		test:is(data.reservationFor.departureGate, 'NG', 'check departure airport gate (update)')
		test:is(data.reservationFor.departureTerminal, 'NT', 'check departure airport terminal (update)')

		resp = flyd_cb('dep_gate', 'fdgate=NNG')
		ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationFor.departureGate, 'NNG', 'check departure airport gate (one update)')

		resp = flyd_cb('dep_gate')
		ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationFor.departureGate, 'NNG', 'check departure airport gate (without update)')
	end)

	test:test('flyd.update.arrival_gate', function(test)
		test:plan(4)

		local resp = flyd_cb('arr_gate', 'fagate=NG&faterm=NT')
		local ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		local data = json.decode(ret[1][2])
		test:is(data.reservationFor.arrivalGate, 'NG', 'check arrival airport gate (update)')
		test:is(data.reservationFor.arrivalTerminal, 'NT', 'check arrival airport terminal (update)')

		resp = flyd_cb('arr_gate', 'fagate=NNG')
		ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationFor.arrivalGate, 'NNG', 'check arrival airport gate (one update)')

		resp = flyd_cb('arr_gate')
		ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationFor.arrivalGate, 'NNG', 'check arrival airport gate (without update)')
	end)

	test:test('flyd.update.delay', function(test)
		test:plan(5)

		-- rule_dep_time, fs_dep_time
		local resp = flyd_cb('delay')
		test:is(resp.status, 400, 'check status for invalid delay')

		resp = flyd_cb('delay', 'rdep=2015-07-20T00:00:00')
		test:is(resp.status, 200, 'check status for ok delay (without update)')

		resp = flyd_cb('delay', 'rdep=bla:bla:bla')
		test:is(resp.status, 400, 'check status for invalid date')

		resp = flyd_cb('delay', 'rdep=2016-01-01T00:00:00')
		test:is(resp.status, 200, 'check status for success update')

		local ret = flyd_get(conn, {{ 'CODE', 'AA', 100, 1, 1, 2016 }}, 'en')
		test:isnt(ret[1][2], 'null', 'check for new record')
	end)

	test:test('flyd.update.diverted', function(test)
		test:plan(3)

		local resp = flyd_divert('rcarrier=RC&rnum=1&rdep_iata=RDEP&rarr_iata=RARR')
		local ret = flyd_get(conn, { 'CODE|RC|1|20|7|2015' }, 'en')
		local data = json.decode(ret[1][2])
		test:is(data.reservationFor.departureAirport.iataCode, 'RDEP', 'check for new record')
		test:is(data.reservationFor.arrivalAirport.iataCode, 'RARR', 'check for new record')

		resp = flyd_divert('rdep=invalid_date_field')
		test:is(resp.status, 400, 'check invalid date in divert cb')
	end)

	test:test('flyd.update.cancel', function(test)
		test:plan(5)

		local resp = flyd_cb('can')
		test:is(resp.status, 200, 'check status for cancel')

		local ret = flyd_get(conn, { 'CODE|AA|100|1|1|2016' }, 'en')
		local data = json.decode(ret[1][2])
		test:is(data.reservationStatus, 'http://schema.org/ReservationCancelled', 'check cancel of new record')

		ret = flyd_get(conn, { 'CODE|AA|100|20|7|2015' }, 'en')
		data = json.decode(ret[1][2])
		test:is(data.reservationStatus, 'http://schema.org/ReservationCancelled', 'check cancel of old record')

		resp = flyd_cb('delay', 'rule_dep_time=2017-01-01T00:00:00')
		test:is(resp.status, 200, 'check status for success update')

		ret = flyd_get(conn, { 'CODE|AA|100|1|1|2017' }, 'en')
		test:is(ret[1][2], 'null', 'check for update after cancel')
	end)
end)
test:check()

deinitialize_test(conn)
