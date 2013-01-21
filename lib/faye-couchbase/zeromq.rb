# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/


require 'em-zeromq'
require 'json'
require 'singleton'



#
# Couchbase => nodes
#


module Faye
class ZeroServer
	include Singleton
	
	
	def init(port, nodes, callback = nil, &block)
		return if @context
		
		callback ||= block
		@nodes = []
		
		pub_socket
		sub_socket.on(:message) { |part|
			callback.call(part.copy_out_string)
			part.close
		}
		
		register_nodes(nodes)
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
	
	
	def register_nodes(nodes)
		#
		# TODO:: find all the nodes that are not known and register them
		#	Then find all the nodes no longer listed and unregister them
		#
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
		channels.each do |channel|
			pub_socket.send_msg(channel)
		end
	end
	
	
	#
	# Helper functions
	#
	def pub_socket
		@pub_socket ||= begin
			pub_socket = context.socket(ZMQ::PUB)
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
	
	
	private
	
	
	def disconnect_from(ip)
		port = @options[:zeromq_port]	# needs to be local as we lose access to self
		@sub_socket.instance_eval { @socket.disconnect("tcp://#{ip}:#{port}") }
	end
	
	
	def context
		@context ||= EM::ZeroMQ::Context.new(1)
	end	
end
end
