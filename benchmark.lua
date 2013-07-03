
--
-- simple Tarantool benchmarking framework
--

benchmark = {}
benchmark.tests = {}
benchmark.init = function()
	local ffi = require("ffi")
	ffi.cdef[[
		typedef long time_t;
		typedef struct timeval {
			time_t tv_sec;
			time_t tv_usec;
		} timeval;
		int gettimeofday(struct timeval *t, void *tzp);
	]]
	benchmark.now = function()
		local t = ffi.new("timeval")
		ffi.C.gettimeofday(t, nil)
		return tonumber(t.tv_sec * 1000 + (t.tv_usec / 1000))
	end
end
benchmark.add = function(name, f)
	benchmark.tests[name] = f
end
benchmark.run = function(count)
	print("Benchmarking (count: ", count, ")")
	print("")
	for name,func in pairs(benchmark.tests) do
		print(">>> ", name)
		local start = benchmark.now()
		for key = 1,count do
			func(key)
		end
		local diff = benchmark.now() - start
		print("rps: ", count / (diff / 1000.0))
		print("")
	end
end

--

benchmark.init()
benchmark.add("insert", function(key)
	box.insert(0, key)
end)
benchmark.run(10000000)
