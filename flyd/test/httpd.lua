#!/usr/bin/env tarantool

local log = require('log')
local json = require('json')
local http = require('http.client')

box.cfg{}

local function prepare_date(day, month, year)
	return string.format("%u-%02u-%0uT00:00:00", year, month, day)
end

local function prepare_answer_ok(carrier, flight, dep_code, arr_code, dep_date,
				 arr_date, dep_gate, arr_gate, dep_term, arr_term)
	return json.encode{
		flightStatuses = {
			{
				carrierFsCode = carrier,
				flightNumber = flight,

				departureAirportFsCode = dep_code,
				arrivalAirportFsCode = arr_code,

				departureDate = {
					dateLocal = dep_date,
				},
				arrivalDate = {
					dateLocal = arr_date,
				},

				airportResources = {
					departureGate = dep_gate,
					arrivalGate = arr_gate,

					departureTerminal = dep_term,
					arrivalTerminal = arr_term
				}
			}
		}
	}
end

local flights = {
	--[[ stucture:
	['carrier'] = {
		['flight'] = {
			['year'] = {
				['month'] = {
					['day'] = {
						hanlders
					}
				}
			}
		}
	},
	--]]

	['AA'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return prepare_answer_ok(carrier, flight, '06C', '06N',
										 prepare_date(day, month, year),
										 prepare_date(day + 1, month, year))
						end,

						new_cb = function(carrier, flight, dep, year, month, day)
							return json.encode{status = 'ok'}
						end
					}
				}
			}
		}
	},

	['ZR'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return prepare_answer_ok(carrier, flight, 'UNKNOWN', 'UNKNOWN',
										 prepare_date(day, month, year),
										 prepare_date(day + 1, month, year),
										 'DG', 'AG', 'DT', 'AT')
						end,

						new_cb = function(carrier, flight, dep, year, month, day)
							return json.encode{status = 'ok'}
						end
					}
				}
			}
		}
	},

	['ERR'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return json.encode{
								error = {
									httpStatusCode = 400,
									errorMessage = 'not found',
								}
							}
						end
					}
				}
			}
		}
	},

	['ERR_JSON'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return prepare_answer_ok(carrier, flight, '06C', '06N',
										 prepare_date(day, month, year),
										 prepare_date(day + 1, month, year),
										 'DG', 'AG', 'DT', 'AT')
						end,

						new_cb = function(carrier, flight, year, month, day)
							return 'non json, just text'
						end
					}
				}
			}
		}
	},

	['ERR_IN_JSON'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return prepare_answer_ok(carrier, flight, '06C', '06N',
										 prepare_date(day, month, year),
										 prepare_date(day + 1, month, year),
										 'DG', 'AG', 'DT', 'AT')
						end,

						new_cb = function(carrier, flight, year, month, day)
							return json.encode{
								error = {
									httpStatusCode = 500,
									errorMessage = 'some error in cb registration',
								}
							}
						end
					}
				}
			}
		}
	},

	['CB'] = {
		['100'] = {
			['2015'] = {
				['07'] = {
					['20'] = {
						req = function(carrier, flight, year, month, day)
							return prepare_answer_ok(carrier, flight, 'DEP', 'ARR',
										 'invalid date returned',
										 prepare_date(day + 1, month, year),
										 'DG', 'AG', 'DT', 'AT')
						end
					}
				}
			}
		}
	}
}

local httpd = require('http.server').new('127.0.0.1', 12345, {
	app_dir = '/tmp/flyd',
})

local flightstats = 'api.flightstats.com/flex/flightstatus/rest/v2/json/flight/status'
local flightstats_args = ':carrier/:flight/dep/:year/:month/:day'
httpd:route({ name = 'flightstats', path = string.format('%s/%s', flightstats, flightstats_args)}, function(req)
	local carrier = req:stash('carrier')
	local flight = req:stash('flight')
	local year = req:stash('year')
	local month = req:stash('month')
	local day = req:stash('day')

	local action = flights[carrier][flight][year][month][day].req

	return {
		status = 200,
		body = action(carrier, flight, year, month, day)
	}
end)

local create_rule = '/api.flightstats.com/flex/alerts/rest/v1/json/create'
local create_rule_args = ':carrier/:flight/from/:dep/departing/:year/:month/:day'
httpd:route({ name = 'rule', path = string.format('%s/%s', create_rule, create_rule_args)}, function(req)
	local carrier = req:stash('carrier')
	local flight = req:stash('flight')
	local dep = req:stash('dep')
	local year = req:stash('year')
	local month = req:stash('month')
	local day = req:stash('day')

	local action = flights[carrier][flight][year][month][day].new_cb
	flights[carrier][flight][year][month][day].cb = req:param('deliverTo')

	if action then
		return {
			status = 200,
			body = action(carrier, flight, dep, year, month, day, cb),
		}
	else
		return {
			status = 500,
			body = json.encode{ error = "can't find action for this request" }
		}
	end
end)

local function request_do(req, cb, rtype)
	local rdep = req:param('rdep')
	local rarr = req:param('rarr')
	local rdep_iata = req:param('rdep_iata')
	local rarr_iata = req:param('rarr_iata')
	local rcarrier = req:param('rcarrier')
	local rnum = req:param('rnum') or req:stash('flight')

	local fdep = req:param('fdep')
	local farr = req:param('farr')
	local fdep_iata = req:param('fdep_iata')
	local farr_iata = req:param('farr_iata')
	local fcarrier = req:param('fcarrier')
	local fnum = req:param('fnum') or req:stash('flight')

	local fdgate = req:param('fdgate')
	local fagate = req:param('fagate')
	local fdterm = req:param('fdterm')
	local faterm = req:param('faterm')

	local body = json.encode{
		alert = {
			event = {
				type = rtype
			},
			rule = {
				flightNumber = rnum,
				departure = rdep,
				arrival = rarr,
				departureAirport = {
					iata = rdep_iata,
				},
				arrivalAirport = {
					iata = rarr_iata,
				},
				carrier = {
					iata = rcarrier,
				},
			},
			flightStatus = {
				flightNumber = fnum,
				carrier = {
					iata = fcarrier
				},
				arrivalAirport = {
					iata = farr_iata,
				},
				departureAirport = {
					iata = fdep_iata,
				},
				departureDate = {
					dateLocal = fdep,
				},
				arrivalDate = {
					dateLocal = farr,
				},

				airportResources = {
					departureGate = fdgate,
					arrivalGate = fagate,
					departureTerminal = fdterm,
					arrivalTerminal = faterm,
				}
			}
		}
	}

	log.info("request body: `%s'", body)

	return http.request('POST', cb, body)
end

local cb_actions = {
	dep_gate = function(req, cb)
		return request_do(req, cb, 'DEPARTURE_GATE')
	end,

	arr_gate = function(req, cb)
		return request_do(req, cb, 'ARRIVAL_GATE')
	end,

	delay = function(req, cb)
		return request_do(req, cb, 'DEPARTURE_DELAY')
	end,

	div = function(req, cb)
		return request_do(req, cb, 'DIVERTED')
	end,

	can = function(req, cb)
		return request_do(req, cb, 'CANCELLED')
	end,
}

local cb_args = ':action/:carrier/:flight/:year/:month/:day'
httpd:route({ name = 'call cb', path = string.format('/cb/%s', cb_args)}, function(req)
	local action = req:stash('action')
	local carrier = req:stash('carrier')
	local flight = req:stash('flight')
	local year = req:stash('year')
	local month = req:stash('month')
	local day = req:stash('day')

	log.info("%s %s %s %s %s %s", action, carrier, flight, year, month, day)
	local cb = flights[carrier][flight][year][month][day].cb
	if not cb then
		return {
			status = 400,
			body = 'cb for this request not found'
		}
	end

	if not cb_actions[action] then
		return {
			status = 400,
			body = 'unknown action'
		}
	end

	return cb_actions[action](req, cb)
end)

httpd:start()
