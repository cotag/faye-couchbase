# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/


require 'couchbase'
require 'em-zeromq'
require 'json'



#
# Couchbase => nodes
#



class Server
	
	
	def initialize(server, options)
		@server  = server
		@options = {
			:zeromq_port => 5555,		# the only ZeroMQ option
			:hostname => "localhost",	# Remaining options are for CouchBase
			:port => 8091
		}.merge!(options)
	end
	
	
	def init
		return if @context
		
		couchbase
		
		pub_socket
		sub_socket.on(:message) { |part|
			@clients = 
		}
	end
	
	
	def disconnect
		return unless @context
		
		# trap("INT")
		@pub_socket.unbind
		@sub_socket.unbind
		@context.terminate
		
		@context	= nil
		@pub_socket = nil
		@sub_socket = nil
	end
	
	
	def subscribe(channel, &callback)
		init
		@sub_socket.subscribe(channel)
		@server.debug 'Subscribed ZeroMQ to channel ?', channel
		callback.call if callback
	end
	
	
	def unsubscribe(channel, &callback)
		@sub_socket.unsubscribe(channel) if @context
		@server.debug 'Unsubscribed ZeroMQ from channel ?', channel
		callback.call if callback
	end
	
	
	def publish(channels)
		init
		pub_socket.send_msg(*channels)
	end
	
	
	#
	# Helper functions
	#
	def pub_socket
		@pub_socket ||= begin
			pub_socket = context.socket(ZMQ::PUB)
			pub_socket.bind("tcp://*:#{@options[:zeromq_port]}")
			pub_socket.bind("tcp://*:#{@options[:zeromq_port]}")
			pub_socket
		end
	end
	
	def sub_socket
		@sub_socket ||= begin
			sub_socket = context.socket(ZMQ::SUB)
			sub_socket.connect("tcp://127.0.0.1:#{@options[:zeromq_port]}")
			sub_socket
		end
	end
	
	def couchbase
		@couchbase ||= begin
			couchbase = Couchbase.connect(@options)
			couchbase.on_error do |opcode, key, exc|
				@server.debug 'Couchbase threw an error: op:? key:? exc:?', opcode, key, exc
			end
			couchbase
		end
	end
	
	
	private
	
	
	def disconnect_from(ip)
		port = @options[:zeromq_port]	# needs to be local as we lose access to self
		@sub_socket.instance_eval { @socket.disconnect("tcp://#{ip}:#{port}") }
	end
	
	
	def context
		@context ||= EM::ZeroMQ::Context.new(1)
	end	
end
