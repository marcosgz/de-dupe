# frozen_string_literal: true

require_relative "lib/de_dupe/version"

Gem::Specification.new do |spec|
  spec.name = "de-dupe"
  spec.version = DeDupe::VERSION
  spec.authors = ["Marcos G. Zimmermann"]
  spec.email = ["mgzmaster@gmail.com"]

  spec.summary = "Distributed deduplication and locking using Redis Sorted Sets"
  spec.description = <<~DESC
    DeDupe is a Ruby gem for distributed deduplication and locking using Redis Sorted Sets.
    It provides a simple and efficient way to prevent duplicate execution of tasks across
    multiple processes or servers, with automatic TTL-based expiration and cleanup.
  DESC
  spec.homepage = "https://github.com/marcosgz/de-dupe"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.7"

  if spec.respond_to?(:metadata)
    spec.metadata["homepage_uri"] = spec.homepage
    spec.metadata["source_code_uri"] = "https://github.com/marcosgz/de-dupe"
    spec.metadata["changelog_uri"] = "https://github.com/marcosgz/de-dupe/blob/main/CHANGELOG.md"
  end

  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end

  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "redis"
  spec.add_dependency "zeitwerk"
  spec.add_development_dependency "standard"
end
