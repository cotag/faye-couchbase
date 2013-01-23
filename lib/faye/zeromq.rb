# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/
# # TODO:: Socket.ip_address_list.detect {|intf| intf.ipv4_private?}
# -> lets simplify to a single ip address


require 'em-zeromq'
require 'json'



#
# Couchbase => nodes
#


module Faye
	class ZeroServer
	
	
		def initialize(options)
			@options = options
			@discovery = NodeSignaling.new(options)
		end
		
		
		def init
			return if @context
			
			pub_socket(options[:com_port] || )
			
			sub_socket(options[:com_port] || 20789).on(:message) { |part|
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
		
		
		def publish(channels)
			init
			channels.each do |channel|
				pub_socket.send_msg(channel)
			end
		end
		
		
		def disconnect_from(ip)
			port = @options[:zeromq_port]	# needs to be local as we lose access to self in eval
			@sub_socket.instance_eval { @socket.disconnect("tcp://#{ip}:#{port}") }
		end
		
		
		def context
			@context ||= EM::ZeroMQ::Context.new(1)
		end
	end
	
	
	
	
	
	
	#
	# This is in effect our node discovery class
	#
	class NodeSignaling
		
		def initialize(options)
			@options = options
			@peers = []
		end
		
		def init
			return if @discoverable
			
			@discoverable = Faye::ServerNode.new
			@discoverable.ip_addresses = []
			
			#
			# @heartbeat every 1min to let other nodes know we are here
			# if the model can't be found we must assume we have disconnected
			#	(it's been > 2min since last heart beat as ttl is 2min)
			#
			@heartbeat = EventMachine.add_periodic_timer( 60, &method(:check_pulse) )
			
			#
			# Inform other nodes of our presence
			#
			get_node_list
			check_pulse
		end
		
		def disconnect
			return unless @discoverable
			
			#
			# Update the database
			#
			@heartbeat.cancel
			begin
				@discoverable.delete
				Faye::ServerNode.all(false)	# trigger a cache re-build
			rescue
			ensure
				@discoverable = nil
				@heartbeat = nil
				@peers = []
			end
			
			#
			# Shutdown the sockets and signal an update
			#
			return unless @context
			@sig_socket.unbind
			@sig_socket = nil
			@context.terminate
			@context	= nil
		end
		
		
		def on_subscribe(&block)
			@subscriber = block
		end
		
		def on_unsubscribe(&block)
			@unsubscriber = block
		end
		
		def on_signal(&block)
			@signal_handler = block
		end
		
		
		def signal_peers(message)
			signal(@peers, message)
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
					@discoverable.touch unless @discoverable.id.nil?
				else
					@discoverable.ip_addresses = current_ips
					@discoverable.save!					# this should set ttl - other nodes will update within the min
					EventMachine.defer do
						# We call false here as couchbase doesn't update indexes on ttl data (23rd Jan 2013)
						#	(will always be a comparatively small data-set)
						Faye::ServerNode.all(false)
						signal_peers('ping')
					end
				end
			rescue
				disconnect
				init				# attempt recovery
			end
		end
		
		
		def signal(peers, message)
			return unless @discoverable	# make sure we've initialised
			EventMachine.schedule do
				#
				# TODO:: create a socket,
				#	connect to all the known peers,
				#	send them an update signal,
				#	shutdown the socket
				#
			end
		end
		
		
		def get_node_list
			ips = []
			
			nodes = Faye::ServerNode.all
			nodes.each do |node|
				ips += node.ip_addresses if node.id != @discoverable.id
			end
			
			#
			# TODO:: use sets here instead of arrays for speed
			#
			difference1 = @peers - ips
			difference2 = ips - @peers
				
			#
			# subscribe / unsubscribe to nodes from the differences
			#
			@peers = ips
			@unsubscriber.call(difference1) unless difference1.empty?
			@subscriber.call(difference2) unless difference2.empty?
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
		
		
		def sig_socket(port)
			@sig_socket ||= begin
				sig_socket = context.socket(ZMQ::REP)
				sig_socket.connect("tcp://*:#{port}")
				sig_socket
			end
		end
		
		
		def context
			@context ||= EM::ZeroMQ::Context.new(1)
		end
		
	end
end
