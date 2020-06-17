# Copyright, 2017, by Samuel G. D. Williams. <http://www.codeotaku.com>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

require_relative 'clock'

module Async
	class Scheduler
		if Thread.instance_methods.include?(:scheduler)
			def self.supported?
				true
			end
		else
			def self.supported?
				false
			end
		end
		
		def initialize(reactor)
			@reactor = reactor
			@blocking_started_at = nil
			
			@wrappers = nil
			@ios = nil
		end
		
		def set!
			if thread = Thread.current
				@ios = {}
				@wrappers = {}
				
				thread.scheduler = self
			end
		end
		
		def clear!
			if thread = Thread.current
				# Because these instances are created with `autoclose: false`, this does not close the underlying file descriptor:
				@ios&.each_value(&:close)
				
				@wrappers = nil
				@ios = nil
				
				thread.scheduler = nil
			end
		end
		
		private def from_io(io)
			@wrappers[io] ||= Wrapper.new(io, @reactor)
		end
		
		private def from_fd(fd)
			@ios[fd] ||= ::IO.for_fd(fd, autoclose: false)
		end
		
		def wait_readable(io, timeout = nil)
			wrapper = from_io(io)
			wrapper.wait_readable(timeout)
		ensure
			wrapper.reactor = nil
		end
		
		def wait_writable(io, timeout = nil)
			wrapper = from_io(io)
			wrapper.wait_writable(timeout)
		ensure
			wrapper.reactor = nil
		end
		
		def wait_any(io, events, timeout = nil)
			wrapper = from_io(io)
			wrapper.wait_any(timeout)
		ensure
			wrapper.reactor = nil
		end
		
		def wait_readable_fd(fd)
			wait_readable(from_fd(fd))
		end

		def wait_writable_fd(fd)
			wait_writable(from_fd(fd))
		end

		def wait_for_single_fd(fd, events, duration)
			wait_any(from_fd(fd), events, duration)
		end
		
		def wait_sleep(duration)
			@reactor.sleep(duration)
		end
		
		def enter_blocking_region
			@blocking_started_at = Clock.now
		end
		
		def exit_blocking_region
			duration = Clock.now - @blocking_started_at
			
			if duration > 0.1
				what = caller.first
				
				warn "Blocking for #{duration.round(4)}s in #{what}." if $VERBOSE
			end
		end
		
		def fiber(&block)
			task = Task.new(&block)
			
			task.resume
			
			return task.fiber
		end
	end
end
