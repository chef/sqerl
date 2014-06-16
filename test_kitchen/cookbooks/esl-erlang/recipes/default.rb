unless node['platform'] == 'ubuntu'
  Chef::Application.fatal! 'The esl-erlang cookbook only supports Ubuntu'
end

distrib_name = case node['platform_version']
when '13.10'
  'saucy'
when '13.04'
  'raring'
when '12.10'
  'quantal'
when '12.04'
  'precise'
else
  Chef::Application.fatal! 'Unsupported Ubuntu version'
end

type = node['kernel']['machine'] == 'x86_64' ? 'amd64' : 'i386'

# they have a weird way to name their URLs...
version = node['esl-erlang']['version'].gsub(/^R(\d+)([A-Z])(?:0?(\d+))?$/, '\1.\2.\3').downcase.chomp('.')

pkg_name = "esl-erlang_#{version}-1~ubuntu~#{distrib_name}_#{type}.deb"
pkg_url = "https://packages.erlang-solutions.com/site/esl/esl-erlang/FLAVOUR_1_general/#{pkg_name}"
pkg_path = ::File.join Chef::Config[:file_cache_path], pkg_name

# if you find a better way to install a .deb with its dependencies, please let me know :-(
# TODO that would be cool as an attribute for the dpkg_package resource
bash 'install-esl-erlang' do
  code "dpkg -i '#{pkg_path}' || (" + [
    '[ $? -eq 1 ]',
    'apt-get update',
    'apt-get -fy install',
    "dpkg -i '#{pkg_path}'"
  ].join(' && ') + ')'
  action :nothing
end

remote_file pkg_path do
  source pkg_url
  not_if "erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell | grep #{node['esl-erlang']['version']}"
  notifies :run, 'bash[install-esl-erlang]'
end
