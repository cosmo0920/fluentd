require_relative 'helper'
require 'openssl'
require 'fluent/test/driver/input'

class CertStoreLoaderTest < ::Test::Unit::TestCase
  if Fluent.windows?
    include Win32::Certstore::Mixin::ShellOut
  end

  def setup
    omit "Certstore loading feature is Windows only" unless Fluent.windows?
    require 'fluent/certstore_loader'
  end

  def create_loader(cert_logical_store_name)
    log_device = Fluent::Test::DummyLogDevice.new
    logger = ServerEngine::DaemonLogger.new(log_device)
    log = Fluent::Log.new(logger)
    cert_store = OpenSSL::X509::Store.new

    Fluent::CertstoreLoader.new(log, cert_store, cert_logical_store_name)
  end

  def test_load_cert_store
    assert_nothing_raised do
      loader = create_loader("Trust")
      loader.load_cert_store
    end
  end

  def test_load_cert_store_with_noexistent_logical_store_name
    assert_raise(Mixlib::ShellOut::ShellCommandFailed) do
      loader = create_loader("Noexistent")
      loader.load_cert_store
    end
  end

  def test_get_certificate
    store_name = "ROOT"
    loader = create_loader(store_name)
    get_data = powershell_out!("ls Cert:\\LocalMachine\\#{store_name} | Select-Object -ExpandProperty 'Thumbprint'")
    certificate_thumprints = get_data.stdout.split("\r\n")

    thumbprint = certificate_thumprints.first
    openssl_x509_obj = loader.get_certificate(thumbprint)
    assert_true openssl_x509_obj.is_a?(OpenSSL::X509::Certificate)
  end

  def test_get_certificate_with_nonexistent_thumbprint
    loader = create_loader("ROOT")
    assert_nil loader.get_certificate("nonexistent")
  end
end
