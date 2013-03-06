# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/
# # TODO:: http://api.zeromq.org/3-1:zmq-pgm


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
			@discovery.on_subscribe do |ip|
				sub_socket.connect("tcp://#{ip}:#{@options[:com_port] || 20789}")
			end
			@discovery.on_unsubscribe(&method(:disconnect_from))	# As the method isn't exposed by em-zeromq
		end
		
		
		def init
			return if @context
			
			pub_socket(@options[:com_port] || 20789)
			sub_socket(@options[:com_port] || 20789).on(:message) { |part|
				resp = part.copy_out_string
				part.close
				@messenger.call(resp) unless @messenger.nil?
			}
			
			@discovery.init
		end
		
		
		def disconnect
			return unless @context
			
			@discovery.disconnect
			
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
			callback.call if callback
		end
		
		
		def unsubscribe(channel, &callback)
			@sub_socket.unsubscribe(channel) if @context
			callback.call if callback
		end
		
		
		def publish(channels)
			init
			channels.each do |channel|
				pub_socket.send_msg(channel)
			end
		end
		
		
		def on_message(&block)
			@messenger = block
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
		
		
		def disconnect_from(ip)
			port = @options[:com_port] || 20789	# needs to be local as we lose access to self in eval
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
			
			#
			# @heartbeat every 1min to let other nodes know we are here
			# if the model can't be found we must assume we have disconnected
			#	(it's been > 2min since last heart beat as ttl is 2min)
			#
			@heartbeat = EventMachine.add_periodic_timer( 60, &method(:check_pulse) )
			
			
			#
			# Create our signal receiver port
			#
			sig_socket(@options[:sig_port] || 6666).on(:message, &method(:sig_recieved))
			
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
				
				#
				# Shutdown the sockets and signal an update
				#
				return unless @context
				@sig_socket.unbind
				@sig_socket = nil		# TODO:: mutex here to prevent shutting down during signalling (guess it could happen)
				@context.terminate
				@context	= nil
			end
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
			current_ip = ip_address(@options[:ip_v4])
			
			if current_ip == @discoverable.ip_address
				@discoverable.touch unless @discoverable.id.nil?	# This may occur if we don't have an IP
			else
				@discoverable.ip_address = current_ip
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
			init				# attempt recovery (delay this by a few seconds EM)
		end
		
		
		def signal(peers, message)
			return unless @discoverable	# make sure we've initialised
			EventMachine.schedule do
				port = options[:sig_port] || 6666
				signaller = context.socket(ZMQ::REQ)
				peers.each {|ip| signaller.connect("tcp://#{ip}:#{port}") }
				signaller.send_msg(message)
				signaller.unbind
			end
		end
		
		
		def get_node_list
			ips = []
			
			nodes = Faye::ServerNode.all
			nodes.each do |node|
				ips << node.ip_address if node.id != @discoverable.id
			end
			
			#
			# TODO:: use sets here instead of arrays for speed
			#
			unsubscribe = @peers - ips
			subscribe = ips - @peers
			
			#
			# subscribe / unsubscribe to nodes from the differences
			#
			@peers = ips
			@unsubscriber.call(unsubscribe) unless unsubscribe.empty?
			@subscriber.call(subscribe) unless subscribe.empty?
		end
		
		
		def ip_address(ip_v4 = true)
			ip = if ip_v4
				Socket.ip_address_list.detect {|a| a.ipv4? && !a.ipv4_loopback?}
			else
				Socket.ip_address_list.detect {|a| !a.ipv4? && !(a.ipv6_linklocal? || a.ipv6_loopback?)}
			end
			return ip.ip_address unless ip.nil?
			nil
		end
		
		
		def sig_socket(port)
			@sig_socket ||= begin
				sig_socket = context.socket(ZMQ::REP)
				sig_socket.bind("tcp://*:#{port}")
				sig_socket
			end
		end
		
		
		def sig_recieved(part)
			resp = part.copy_out_string
			part.close
			
			if resp == 'ping'
				get_node_list	# We only care for node discovery signals
			else
				@signal_handler.call(resp) if @signal_handler	# Delegate anything else
			end
		end
		
		
		def context
			@context ||= EM::ZeroMQ::Context.new(1)
		end
		
	end
end
