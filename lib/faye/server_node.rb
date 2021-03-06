
module Faye
	class ServerNode < Couchbase::Model
		attribute :cluster_id, :ip_address, :message_port, :signal_port
		validates :ip_address, :presence => true
		validates :message_port, :presence => true	# Publish, Subscribe
		validates :signal_port, :presence => true	# Request, Reply
		
		
		view :by_cluster_id
		
		
		#
		# Ensures every faye node has a unique ID in the database
		#
		define_model_callbacks :save
		before_save :generate_key
		
		def generate_key
			self.cluster_id = ENV['COUCHBASE_CLUSTER'] || 1
			
			while self.id.nil?
				count = self.class.bucket.incr("faye_node:#{self.cluster_id}:count", :create => true)
				theid = "faye_node:#{self.cluster_id}:#{count}"
				self.id = theid if self.class.find_by_id(theid).nil?
			end
		end
		
		
		#
		# So we can be informed if the server was inadvertently disconnected
		#
		def disconnected_callback(callback, &block)
			@disconnected = callback || block
		end
		
		
		#
		# This is used to auto-timeout servers if they stop responding
		#
		defaults :ttl => 120
		
		def touch(ttl = 120)
			ServerNode.bucket.touch(self.id, :ttl => ttl)
		rescue Couchbase::Error::NotFound
			@disconnected.call unless @disconnected.nil?
		end
		
		
		#
		# Find all the faye servers registered in the current cluster
		#
		def self.all(stale = :ok)	# :ok, false, :update_after are all legal
			# Requires :key
			by_cluster_id(:key => (ENV['COUCHBASE_CLUSTER'] || 1), :stale => stale).to_a
		end
	
	end
end
