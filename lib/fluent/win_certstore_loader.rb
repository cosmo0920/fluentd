#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'win32-certstore'
require 'openssl'

class Win32::CertstoreLoader
  attr_reader :cert_store

  include Win32::Certstore::Mixin::ShellOut

  def initialize(log, cert_store, store_name)
    @log = log
    @cert_store = cert_store
    @store_name = store_name
  end

  def load_cert_store
    get_data = powershell_out!("ls Cert:\\LocalMachine\\#{@store_name} | Select-Object -ExpandProperty 'Thumbprint'")
    certificate_thumprints = get_data.stdout.split("\r\n")

    @log.trace "loading Windows Certstore certificates..."
    Win32::Certstore.open(@store_name) do |store|
      certificate_thumprints.each do |thumbprint|
        begin
          x509_certificate_obj = store.get(thumbprint)
          @cert_store.add_cert(x509_certificate_obj)
        rescue OpenSSL::X509::StoreError => e # continue to read
          @log.warn "failed to load certificate(thumbprint: #{thumbprint}) from certstore", error: e
        end
      end
    end
    @log.trace "loaded Windows Certstore certificates."
  end

  def get_certificate(thumbprint)
    Win32::Certstore.open(@store_name) do |store|
      store.get(thumbprint)
    end
  end
end
