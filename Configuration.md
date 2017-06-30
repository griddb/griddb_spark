# Configuration

Here is a list of the properties for GridDB.

|Property|Description|Required|Default Value|
|---|---|---|---|
|gs.user|User name to access GridDB cluster|Required if GridDB is the selected datastore.||
|gs.password|User password to access GridDB cluster|Required if GridDB is the selected datastore.||
|gs.cluster.name|Name of GridDB cluster|Required if GridDB is the selected datastore.||
|gs.notification.address|An IP address (IPV4 only) for receiving a notification used for autodetection of a GridDB master node|Required if using MULTICAST method to connect to GridDB cluster (cannot be specified with neither notification_member nor notification_provider properties at the same time)|239.0.0.1||
|gs.notification.port|An IP port for receiving a notification used for autodetection of a GridDB master node|Required if notification_address is specified|31999||
|gs.notification.member|"List of GridDB cluster nodes. In the form of: (Address1):(Port1),(Address2):(Port2),... "|Required if using FIXED LIST method to connect to GridDB cluster (cannot be specified with neither notification_address nor notification_provider properties at the same time)||
|gs.notification.provider|URL of address provider used to connect to cluster which is configured with PROVIDER mode|Required if using PROVIDER method to connect to GridDB cluster (cannot be specified with neither notification_address nor notification_member properties at the same time)||
|gs.consistency|Can be one of two consistency levels: IMMEDIATE or EVENTUAL|Optional|IMMEDIATE||
|gs.container.cache.size|Size of cache to store GridDB ContainerInfos which are used to get Container without requesting to GridDB |Optional|0|||gs.data.affinity.pattern|GridDB affinity pattern for each container|Optional||
|gs.failover.timeout|The minimum value of waiting time in seconds until a new destination is found in a failover|Optional|60||
|gs.transaction.timeout|The minimum value of transaction timeout time in seconds|Optional|300||
