require 'spec_helper'
require 'faye/zeromq'


#EM.run do
	describe Faye::ZeroServer do

		before :all do
			@server1 = Faye::ZeroServer.new()
			@server2 = Faye::ZeroServer.new({
				:message_port => 20790,			# Slightly different port numbers
				:signal_port => 6667			# To avoid clashes
			})
		end

		after :all do
			#@server1.disconnect
			#@server2.disconnect
			#EM.stop
		end

		it "should be able to subscribe to a channel" do
			EM.run do
				@server1.init
				@server2.init

				EM::Timer.new(5) do
					@server1.subscribe('test') do
						# Callback works
						true.should == true

						@server1.on_message do |message|
							warn message
							EM.stop
						end
						@server2.publish({
							:channels => ['test'], :message_id => 'some message'
						})
					end
				end
			end
		end
	end
#end
