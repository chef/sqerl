%% a bit odd, but field names have to match column names for helper
%% function to work.

-type id() :: binary().

-record(chef_user, {'id',
                    'authz_id',
                    'username',
                    'pubkey_version',
                    'public_key'}).

-define(KEY_VERSION, 0).
-define(CERT_VERSION, 1).

-record(chef_node, {'id',               % guid for object (unique)
		    'authz_id',         % authorization guid (unique)
		    'org_id',           % organization guid
		    'name',             % node name
		    'environment',      % environment
		    'last_updated_by',  % authz guid of last actor to update object
		    'created_at',       % time created at
		    'updated_at',       % time created at
		    'serialized_object' % json blob of object data
		   }).
%% create_table(:nodes) do
%%      String(:id, :primary_key => true, :fixed => true, :size => 32)
%%      String(:authz_id, :null => false, :fixed => true, :size => 32, :unique => true)
%%      String(:org_id, :null => false, :index => true, :fixed => true, :size => 32)
%%      String(:name, :null => false) # index is handled with unique index on org/name combo
%%      String(:environment, :null => false)
%%      blob(:serialized_object)
%%      String(:last_updated_by, :null => false, :fixed => true, :size => 32)
%%      DateTime(:created_at, :null => false)
%%      DateTime(:updated_at, :null => false)

%%      unique([:org_id, :name]) # only one node with a given name in an org
%%      index([:org_id, :environment]) # List all the nodes in an environment
%% end

-record(chef_container, {'id',             % guid for object (unique)
			 'authz_id',       % authorization guid (unique) 
			 'org_id',         % organization guid
			 'name',           % name of container
			 'path',           % 'path' of container (not used? Orig part of inheritance mech?; safe to delete? Yea!)
			 'last_updated_by' % authz guid of last actor to update object
			}).

%
% Possible extra fields include created_at and updated_at (or not)
%
-record(chef_client, {'id',             % guid for object (unique)
		      'authz_id',       % authorization guid (unique) 
		      'org_id',         % organization guid
		      'name',           % name of container
		      'certificate',    % public key cert
		      'last_updated_by' % authz guid of last actor to update object
		     }).


%% This doesn't quite belong here, but rather in a chef_db hrl file.
%% Used as a common data format for actor data (users or clients).
-record(chef_requestor, {
          type = user :: 'user' | 'client',
          authz_id,
          name,
          key_data}).
