$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "faye-couchbase/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "faye-couchbase"
  s.version     = FayeCouchbase::VERSION
  s.authors     = ["Stephen von Takach"]
  s.email       = ["steve@cotag.me"]
  s.homepage    = "http://github.com/cotag/faye-zerobase"
  s.summary     = "ZeroMQ + Couchbase backend engine for Faye."
  s.description = "Provides persistent storage using couchbase and sub / sub messaging using zeromq."

  s.files = Dir["{app,config,db,lib}/**/*"] + ["MIT-LICENSE", "Rakefile", "README.rdoc"]
  s.test_files = Dir["{test,spec}/**/*"]

  s.add_dependency "rails", ">= 3.0.0"
  s.add_dependency "eventmachine", ">= 0.12.0"
  s.add_dependency "em-zeromq"
  s.add_dependency "couchbase"
  s.add_dependency "couchbase-model"

  s.add_development_dependency "rspec"
end
