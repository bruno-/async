# Copyright, 2020, by Samuel G. D. Williams. <http://www.codeotaku.com>
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

require 'async/scheduler'
require 'async/barrier'
require 'net/http'

RSpec.describe Async::Scheduler, if: Async::Scheduler.supported? do
	include_context Async::RSpec::Reactor
	
	it "can intercept sleep" do
		expect(reactor).to receive(:sleep).with(0.001)
		
		sleep(0.001)
	end
	
	describe 'Fiber.schedule' do
		it "can start child task" do
			fiber = Async do
				Fiber.schedule{}
			end.wait
			
			expect(fiber).to_not be_nil
			expect(fiber).to be_kind_of(Fiber)
		end
	end
	
	describe 'Process.wait' do
		it "can wait on child process" do
			expect(reactor.scheduler).to receive(:process_wait).and_call_original
			
			pid = ::Process.spawn("true")
			_, status = Process.wait2(pid)
			expect(status).to be_success
		end
	end
	
	describe 'IO.pipe' do
		let(:message) {"Helloooooo World!"}
		
		it "can send message via pipe" do
			input, output = IO.pipe
			
			reactor.async do
				sleep(0.001)
				
				message.each_char do |character|
					output.write(character)
				end
				
				output.close
			end
			
			expect(input.read).to be == message
			
			input.close
		end
		
		it "can fetch website using Net::HTTP" do
			barrier = Async::Barrier.new
			events = []
			
			3.times do |i|
				barrier.async do
					events << i
					response = Net::HTTP.get(URI "https://www.codeotaku.com/index")
					expect(response).to_not be_nil
					events << i
				end
			end
			
			barrier.wait
			
			# The requests all get started concurrently:
			expect(events.first(3)).to be == [0, 1, 2]
		end
		
		it "can clean up unused wrappers" do
			input, output = IO.pipe
			
			reader = reactor.async do
				input.read
				input.close
				input = nil
			end
			
			output.write(".")
			output.close
			
			reader.wait
			reader = nil
			output = nil
			
			# Oh no...
			10.times do
				GC.start
				break unless reactor.scheduler.wrappers.any?
				sleep(0.001)
			end
			
			expect(reactor.scheduler.wrappers.size).to be == 0
		end

		specify do
			GC.disable # weakmap objects are not cleaned

			input, output = IO.pipe

			puts "input object_id: #{input.object_id}"
			puts "output object_id: #{output.object_id}"

			5.times do |i|
				reader = reactor.async do
					input.read(1)
				end
				
				output.write(".")
				reader.wait

				# Scheduler wrappers are not reused, they just grow.
				expect(reactor.scheduler.wrappers.size).to eq i + 1
			end

			input.close
			output.close
		end
	end
	
	context "with thread" do
		let(:queue) {Thread::Queue.new}
		subject {Thread.new{queue.pop}}
		
		it "can join thread" do
			waiting = 0
			
			3.times do
				Async do
					waiting += 1
					subject.join
					waiting -= 1
				end
			end
			
			expect(waiting).to be == 3
			queue.close
		end
	end
	
	context "with queue" do
		subject {Thread::Queue.new}
		let(:item) {"Hello World"}
		
		it "can pass items between thread and fiber" do
			Async do
				expect(subject.pop).to be == item
			end
			
			Thread.new do
				expect(Fiber).to be_blocking
				subject.push(item)
			end.join
		end
	end
end
