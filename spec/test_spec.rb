require 'spec_helper'
require 'faye/zeromq'

describe Faye::ZeroServer do
	before do
		@zero_server = Faye::ZeroServer.new({
			:ip_v4 => true
		})
	end

	it "should pass" do
		true.should eq(true)
	end
end


describe Faye::NodeSignaling do
	before do
		@signaller = Faye::NodeSignaling.new({
			:ip_v4 => true
		})
	end

	it "should initialise" do
		@signaller.init
	end
end
