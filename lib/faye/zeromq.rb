# http://techno-weenie.net/2011/6/17/zeromq-pub-sub/
# # TODO:: http://api.zeromq.org/3-1:zmq-pgm


require 'em-zeromq'
require 'json'
require 'socket'



DEFAULT_ERROR = ->(error) { raise error }



#
# Couchbase => nodes
#


module Faye
	class ZeroServer
	
		def initialize(options = {})
			@options = {
				:message_port => 20789,
				:signal_port => 6666,
				:ip_v4 => true
			}.merge!(options)
			
			@discovery = NodeSignaling.new(@options)
			@discovery.on_subscribe do |ip|
				sub_socket.connect("tcp://#{ip}")
			end
			@discovery.on_unsubscribe(&method(:disconnect_from))	# As the method isn't exposed by em-zeromq
		end
		
		
		def init
			return if @context
			
			pub_socket
			sub_socket.on(:message) { |part|
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
			#@context.terminate
			
			#@context	= nil
			@pub_socket = nil
			@sub_socket = nil
		end
		
		
		def subscribe(channel)
			init
			@sub_socket.subscribe(channel)
			yield if block_given?	# Callback
		end
		
		
		def unsubscribe(channel)
			@sub_socket.unsubscribe(channel) if @context
			yield if block_given?	# Callback
		end
		
		#
		# message = {:channels => ['array', 'of', 'strings'], :message_id => 'string'}
		#
		def publish(message)
			init
			message[:channels].each do |channel|
				pub_socket.send_msg(channel, message[:message_id])
			end
		rescue => error
			block_given? ? yield(error) : DEFAULT_ERROR.call(error)
		end
		
		
		def on_message(&block)
			@messenger = block
		end
		
		
		private
		
		
		#
		# Helper functions
		#
		def pub_socket
			@pub_socket ||= begin
				pub_socket = context.socket(ZMQ::PUB)
				pub_socket.bind("tcp://*:#{@options[:message_port]}")
				pub_socket
			end
		end
		
		def sub_socket
			@sub_socket ||= begin
				sub_socket = context.socket(ZMQ::SUB)
				sub_socket.connect("tcp://127.0.0.1:#{@options[:message_port]}")
				sub_socket
			end
		end
		
		
		def disconnect_from(ip)
			@sub_socket.instance_eval { @socket.disconnect("tcp://#{ip}") }
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
			sig_socket(@options[:signal_port]).on(:message, &method(:sig_recieved))
			
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
				#@context.terminate
				#@context	= nil
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

			warn "ip: #{current_ip.nil?} and old #{@discoverable.ip_address}"
			
			if current_ip == @discoverable.ip_address
				@discoverable.touch unless @discoverable.id.nil?	# This may occur if we don't have an IP
				# TODO:: in the case where ip is nil we should be polling for an update in status
			else
				warn "Saving node discovery"
				@discoverable.ip_address = current_ip
				@discoverable.message_port = @options[:message_port]
				@discoverable.signal_port = @options[:signal_port]
				@discoverable.save!					# this should set ttl - other nodes will update within the min
				EventMachine.defer do
					# We call false here as couchbase doesn't update indexes on ttl data (23rd Jan 2013)
					#	(will always be a comparatively small data-set)
					Faye::ServerNode.all(false)
					signal_peers('ping')
				end
			end
			
		#rescue
		#	disconnect
		#	init				# attempt recovery (delay this by a few seconds EM)
		end
		
		
		def signal_peers(message)
			return unless @discoverable	# make sure we've initialised
			EventMachine.schedule do
				warn "Signalling peers"
				signaller = context.socket(ZMQ::PUSH)
				@nodes.each do |node|
					warn " -> tcp://#{node.ip_address}:#{node.signal_port}"
					signaller.connect("tcp://#{node.ip_address}:#{node.signal_port}") if node.id != @discoverable.id && !node.ip_address.nil?
				end
				EM::Timer.new(1) do
					signaller.send_msg(message)
				end
				signaller.unbind
			end
		end
		
		
		def get_node_list
			ips = []
			
			@nodes = Faye::ServerNode.all
			@nodes.each do |node|
				ips << "#{node.ip_address}:#{node.message_port}" unless node.ip_address.nil?
			end
			
			#
			# TODO:: use sets here instead of arrays for speed
			#
			unsubscribe = @peers - ips
			subscribe = ips - @peers
			
			#
			# subscribe / unsubscribe to nodes from the differences
			#
			warn "Getting node list #{ips} from #{@nodes}"
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
				sig_socket = context.socket(ZMQ::PULL)
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
