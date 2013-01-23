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
			@peers = []
		end
		
		def init
			return if @discoverable
			
			@discoverable = Faye::FayeServer.new
			@discoverable.ip_addresses = ip_addresses(@options[:ip_v4])
			@discoverable.save!	# trigger an error if we are not discoverable
			
			#
			# @heartbeat every 2min to let other nodes know you are here
			# => if the model can't be found we must assume we have disconnected
			# => TODO:: should we then try to reconnect? or kill this instance? How to do this gracefully
			#
			@heartbeat = EventMachine.add_periodic_timer( 60, &method(:check_pulse) )
			
			#
			# Inform other nodes of our presence
			# We call false here as couchbase doesn't update indexes on ttl data (23rd Jan 2013)
			#	(will always be a comparatively small data-set)
			#
			other_nodes = Faye::FayeServer.all(false)
			get_node_list(other_nodes)
			# TODO:: signal the other nodes of our presence here
		end
		
		def disconnect
			return unless @discoverable
			
			@heartbeat.cancel
			begin
				@discoverable.delete
				Faye::FayeServer.all(:update_after)	# trigger a cache re-build however execute this request quickly
			rescue
			ensure
				@discoverable = nil
				@heartbeat = nil
				@peers = []
			end
		end
		
		
		protected
		
		
		def check_pulse
			current_ips = ip_addresses(@options[:ip_v4])
			
			#
			# TODO:: use sets here instead of arrays for speed
			#
			difference1 = current_ips - @discoverable.ip_addresses
			difference2 = @discoverable.ip_addresses - current_ips
			
			
			begin
				if difference1.empty? && difference2.empty?
					@discoverable.touch
				else
					@discoverable.ip_addresses = current_ips
					@discoverable.save!					# this should set ttl - other nodes will update within the min
					Faye::FayeServer.all(:update_after)
					# Trigger an update here?
				end
			rescue
				disconnect
				init				# attempt recovery
			end
		end
		
		
		def get_node_list(nodes = nil)
			ips = []
			
			nodes = Faye::FayeServer.all if nodes.nil?
			nodes.each do |node|
				ips += node.ip_addresses if node.id != @discoverable.id
			end
			
			#
			# TODO:: use sets here instead of arrays for speed
			#
			difference1 = @peers - ips
			difference2 = ips - @peers
			if !(difference1.empty? && difference2.empty?)
				@peers = ips
				
				#
				# TODO:: subscribe / unsubscribe to nodes from the differences
				#
			end
		end
		
		
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
					if !a.ipv4? && !(a.ipv6_linklocal? || a.ipv6_loopback?)
						list << a
					end
				end
			end
			
			list
		end
		
	end
end
