# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/


require 'em-zeromq'
require 'json'



#
# Couchbase => nodes
#


module Faye
	class ZeroServer
		
		
		def init(options, signal, notify)
			return if @context
			
			@nodes = []
			
			pub_socket(options[:com_port] || 5555)
			sub_socket(options[:com_port] || 5555).on(:message) { |part|
				resp = part.copy_out_string
				part.close
				notify.call(resp)
			}
			
			sig_socket(options[:sig_port] || 6666).on(:message) { |part|
				resp = part.copy_out_string
				part.close
				signal.call(resp)
			}
			
			register_nodes(options[:nodes]) unless options[:nodes].nil?
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
		
		
		
		private
		
		
		#
		# Helper functions
		#
		def pub_socket(port)
			@pub_socket ||= begin
				pub_socket = context.socket(ZMQ::PUB)
				pub_socket.bind("tcp://*:#{port}")
				pub_socket
			end
		end
		
		def sub_socket(port)
			@sub_socket ||= begin
				sub_socket = context.socket(ZMQ::SUB)
				sub_socket.connect("tcp://127.0.0.1:#{port}")
				sub_socket
			end
		end
		
		def sig_socket(port)
			@sig_socket ||= begin
				sig_socket = context.socket(ZMQ::REP)
				sig_socket.connect("tcp://*:#{port}")
				sig_socket
			end
		end
		
		
		def publish(channels)
			init
			channels.each do |channel|
				pub_socket.send_msg(channel)
			end
		end
		
		
		def disconnect_from(ip)
			port = @options[:zeromq_port]	# needs to be local as we lose access to self
			@sub_socket.instance_eval { @socket.disconnect("tcp://#{ip}:#{port}") }
		end
		
		
		def context
			@context ||= EM::ZeroMQ::Context.new(1)
		end	
	end
	
	
	class NodeSignaling
		
		def initialize(options)
			@options = options
		end
		
		def init(bucket)
			model = Faye::FayeServer.new
			model.ip_addresses = ip_addresses(@options[:ip_v4])
			model.save!	# trigger an error if we have no ip address
			
			#
			# TODO:: @heartbeat = EventMachine.add_periodic_timer {model.touch}
			# => if the model can't be found we must assume we have disconnected and are re-connecting
			# => 
			#
			
			other_nodes = model.class.all(:update_after)
		end
		
		protected
		
		def ip_addresses(ip_v4 = true)
			list = []
			
			if ip_v4
				Socket.ip_address_list.each do |a|
					if a.ipv4? && !a.ipv4_loopback?
						list << a
					end
				end
			else
				Socket.ip_address_list.each do |a|
					if !a.ipv4? && !(a.ipv6_sitelocal? || a.ipv6_linklocal? || a.ipv6_loopback?)
						list << a
					end
				end
			end
			
			list
		end
		
	end
end
