
Couchbase::Model::Configuration.design_documents_paths << File.dirname(__FILE__)
class FayeServer < Couchbase::Model
	attribute :cluster_id, :ip_addresses
	validates :ip_addresses, :presence => true
	
	
	view :by_cluster_id
	
	
	#
	# Ensures every faye node has a unique ID in the database
	#
	define_model_callbacks :save
	before_save :generate_key
	
	def generate_key
		self.cluster_id = ENV['COUCHBASE_CLUSTER'] || 1
		
		while self.id.nil?
			count = self.class.bucket.incr("faye_server:#{self.cluster_id}:count", :create => true)
			theid = "faye_server:#{self.cluster_id}:#{count}"
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
	# This is used to auto-timeout servers that have crashed
	#
	defaults :ttl => 120
	
	def touch(ttl = 120)
		FayeServer.bucket.touch(self.id, :ttl => ttl)
	rescue Couchbase::Error::NotFound
		@disconnected.call unless @disconnected.nil?
	end
	
	
	#
	# Find all the faye servers registered in the current cluster
	#
	def self.all
		# Requires :key
		by_cluster_id(:key => ENV['COUCHBASE_CLUSTER'] || 1).to_a
	end

end
