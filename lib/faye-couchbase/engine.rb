module FayeCouchbase
	class Engine < ::Rails::Engine
		config.after_initialize do
			Couchbase::Model::Configuration.design_documents_paths << Pathname.new(File.dirname(__FILE__)).to_s
		end
	end
end
